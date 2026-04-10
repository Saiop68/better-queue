# BetterQueue: Complete Architecture & Codebase Guide

BetterQueue is a high-performance, distributed rate limiter and job queue designed specifically to replace `Bottleneck` in large-scale Node.js applications (like Discord bots operating in 10,000+ guilds). 

It solves the fundamental flaws of Bottleneck at scale: **memory leaks from per-queue closures** and **CPU starvation from tens of thousands of idle `setInterval` timers**.

---

## 1. The Core Philosophy

In a typical 10,000 guild bot with 40 features per guild, you might have **400,000 potential rate limiters**. 

*   **Bottleneck's Approach:** 400,000 JS objects, each running a `setInterval` heartbeat every 250ms, storing waiting jobs as closures in RAM. Result: OOM crashes and uncollectable memory overhead.
*   **BetterQueue's Approach:** **Zero per-limiter timers**. All queue state (waiting jobs, running counters, token buckets) lives purely in Redis. A single global "Worker Pool" pulls work using blocking Redis commands (`BRPOP`). If a guild is idle, its RAM and CPU footprint is exactly zero.

---

## 2. Redis Key Structure (The Source of Truth)

Each active queue (e.g., `vcrole-voice-join` for `guild_123`) has a unique `id`. In Redis, this expands into several keys.

### Volatile Runtime Keys (Deleted immediately when the queue empties)
*   **`bq:{id}:jobs` (ZSET)**: Pending jobs waiting to execute. Score = timestamp to execute.
*   **`bq:{id}:running` (SET)**: IDs of jobs currently executing in Node.js. Used to enforce `maxConcurrent`.
*   **`bq:{id}:inflight` (HASH)**: Crash recovery bookmark. JobId -> payload.
*   **`bq:{id}:lastRun` (STRING)**: Timestamp of the last time a job was dequeued. Used to enforce `minTime`.

### Persistent Keys (Auto-expire after 1 hour of inactivity)
*   **`bq:{id}:meta` (HASH)**: The configuration for this specific queue (maxConcurrent, minTime, reservoir). Survives when the queue empties so cold-starts are fast.

### Global Keys
*   **`bq:workers:notify` (LIST)**: The central channel. When a queue has a job ready to run, its `id` is pushed here. Workers `BRPOP` on this list.
*   **`bq:global:wakeup` (ZSET)**: The central waiting room for rate-limited queues. If a queue hits its `maxConcurrent` or `minTime`, it is parked here with a timestamp.

---

## 3. The Codebase Step-by-Step

### `LimiterManager.ts` (The Drop-in API Wrapper)
This is what your bot interacts with. It was designed to have the exact same API as your old Bottleneck manager so you don't have to rewrite your bot logic.
*   **`registerFeatureConfig()`**: Stores your default rate limits (e.g., `maxConcurrent: 1`, `minTime: 1000`) in a simple memory map.
*   **`getLimiter()`**: Checks a local `LRUCache`. If not found, instantiates a lightweight `DistributedLimiter`. 
*   **`LimiterWrapper`**: Exposes the `.schedule()` method. It wraps your job in a `Promise.race` to enforce the `timeout` parameter client-side.

### `DistributedLimiter.ts` (The Redis Delegate)
A very thin class. It doesn't hold timers or job arrays. 
When you call `limiter.schedule(fn)`, it:
1. Generates a unique Job ID.
2. Puts the `fn` closure into the WorkerPool's `pendingJobs` Map in memory. (This is necessary because Redis can't store actual JS functions).
3. Calls the `bqEnqueue` Lua script to tell Redis to queue the job.

### `WorkerPool.ts` (The Engine)
This is where the magic happens. You instantiate ONE of these per Node.js process.
*   **The Workers (`workerLoop`)**: An array of async infinite loops. Each loop does exactly one thing: `await client.brpop(KEYS.notify)`. When a queue ID pops out, the worker delegates to the `bqDequeue` Lua script. If successful, it pulls the `fn` closure from `pendingJobs` and executes it.
*   **The Ticker (`startTicker`)**: A single `setInterval` running every 50ms. It runs the `bqWakeupCheck` Lua script to see if any rate-limited queues in `bq:global:wakeup` are ready to run.
*   **Error Handling**: If a job throws, it checks `retryCount`. If retries remain, it recalculates an exponential backoff time and re-enqueues the job. If out of retries, it resolves the promise with a `JobMaxRetriesError`.

### `RedisClient.ts`
Registers all our custom Lua scripts using `ioredis.defineCommand`. This guarantees TypeScript type safety for our highly specific Lua arguments.

---

## 4. The Lua Scripts (The Brains)

Redis Lua scripts are **atomic**. While one runs, no other client can interfere. This prevents massive race conditions when 10 workers try to grab jobs at the exact same millisecond. 

#### `enqueue.lua`
1. Uses `TIME` to get the server time.
2. `ZADD`s the job payload to `bq:{id}:jobs`.
3. Refreshes the `EXPIRE` TTLs on the queue so it stays alive.
4. `LPUSH`es the queue ID to `bq:workers:notify` to wake up a sleeping worker.

#### `dequeue.lua` (The Gatekeeper)
This script is called by a worker when it wakes up. It decides if a job is *allowed* to run.
1. **Concurrency Check**: `SCARD running` -> Is it >= `maxConcurrent`? If yes, return 'concurrent_full'.
2. **MinTime Check**: `GET lastRun` -> Is `now - lastRun < minTime`? If yes, return 'rate_limited'.
3. **Reservoir Check**: Are there tokens left? If 0, return 'empty_reservoir'.
4. **Execution**: If all checks pass:
   * `ZREM` the job from the pending ZSET.
   * `SADD` the job to the `running` SET.
   * `HSET` the job into the `inflight` HASH (for crash recovery).
   * Update the `lastRun` timestamp.
   * Return the job payload to the worker.

#### `complete.lua` (The Cleanup Crew)
Called when a worker finishes a job (successfully or by throwing/timing out).
1. `SREM` the job from the `running` SET.
2. `HDEL` the job from the `inflight` HASH.
3. Check if the queue is now completely empty (`ZCARD jobs == 0` AND `SCARD running == 0`).
   * **If Empty**: Immediately `DEL` the jobs ZSET, running SET, inflight HASH, and lastRun STRING. This instantly frees Redis memory for idle guilds. The `meta` key is kept alive but given a 1-hour TTL.
   * **If Not Empty**: `LPUSH` back to the notify queue to wake a worker for the next job, and refresh the TTL on all active keys to 1 hour.

#### `wakeup_check.lua`
Called by the global ticker every 50ms.
1. `ZRANGEBYSCORE wakeup -inf <current_time>` -> gets queues that were previously rate-limited but are now eligible to run again.
2. `ZREMRANGEBYSCORE` to remove them from the sleeping list.
3. `LPUSH`es them into the `notify` list so workers can process them.

#### `recover.lua`
Called only once when the bot starts up.
1. Looks for any queue ID pattern in Redis.
2. Checks the `inflight` HASHes. If a job has been sitting in `inflight` longer than `visibilityTimeoutMs` (e.g., 1 hour), it assumes the previous Node.js process crashed mid-execution.
3. Takes the orphaned job, removes it from the `running` SET, and pushes it back into the pending `jobs` ZSET so a worker can retry it.

---

## 5. Day in the Life of a Rate-Limited Job 

**Scenario:** Guild 123 has `maxConcurrent: 1`, `minTime: 1000`. Two members join at the exact same time.

**T=0ms (Member 1 & 2 Join)**
* Your bot calls `.schedule()` twice.
* `enqueue.lua` runs twice. Both jobs are parked in the `bq:vcrole:123:jobs` ZSET.
* Two notifications are sent to `bq:workers:notify`.

**T=5ms (Worker A Wakes Up)**
* Worker A pops "vcrole:123" from the notify list.
* Runs `dequeue.lua`. `running` is 0. Job 1 is allowed.
* Job 1 is moved to the `running` SET.
* Node.js starts executing Job 1's function (adding the role via Discord HTTP).

**T=10ms (Worker B Wakes Up)**
* Worker B pops the second "vcrole:123" notification.
* Runs `dequeue.lua`. `running` is 1. `maxConcurrent` is 1!
* **Denied.** Script returns `'concurrent_full'`. 
* Worker B gives up and goes back to sleep. Job 2 stays in the Redis ZSET.

**T=150ms (Job 1 Completes)**
* Discord responds. Job 1 is done.
* `complete.lua` runs. Removes Job 1 from `running` SET.
* Script checks: Are there more jobs? Yes, Job 2 is still waiting.
* Script pushes a new notification to `bq:workers:notify`.

**T=155ms (Worker A or B Wakes Up)**
* Pops "vcrole:123". Runs `dequeue.lua`.
* Concurrency is now 0 (Job 1 finished). 
* **BUT**, `lastRun` was T=5ms. `minTime` is 1000ms. 
* T=155ms - 5ms = 150ms. 150ms < 1000ms.
* **Denied.** Script returns `'rate_limited'`. Returns the exact time Job 2 *should* run: T=1005ms.

**T=160ms (Worker parks the queue)**
* Worker receives the 'rate_limited' signal and the projected execution time.
* It calls `WorkerPool.scheduleWakeup(queueId, 1005)`.
* `zadd bq:global:wakeup 1005 vcrole:123`.

**T=1000ms (The Ticker)**
* The 50ms ticker checks `bq:global:wakeup`.
* 1000 >= 1005? No. Nothing happens.

**T=1050ms (The Ticker)**
* Ticker checks. 1050 >= 1005? Yes!
* `wakeup_check.lua` pulls "vcrole:123" out and pushes it to `bq:workers:notify`.

**T=1055ms (Worker Wakes Up)**
* Pops "vcrole:123". Runs `dequeue.lua`.
* Concurrency is 0. Time since `lastRun` is 1050ms (which is > 1000ms).
* **Allowed.** Job 2 executes.

**T=1200ms (Job 2 Completes)**
* `complete.lua` runs.
* Checks: Are there more jobs? No. Is anything running? No.
* The queue is empty.
* **Immediate Cleanup**: The volatile running, inflight, and jobs keys are instantly deleted. The meta config key is given a 1 hour TTL and left alone safely.
