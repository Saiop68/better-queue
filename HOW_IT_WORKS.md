# BetterQueue: Complete Architecture & Codebase Guide

BetterQueue is a high-performance, distributed rate limiter and job queue designed to replace `Bottleneck` in large-scale Node.js applications.

It solves two structural problems with Bottleneck at scale: **memory growth from per-queue state** and **CPU starvation from tens of thousands of idle `setInterval` timers**.

---

## 1. The Core Philosophy

In a typical 10,000-scope application with 40 features per scope, you might have **400,000 potential rate limiters**.

*   **Bottleneck's approach:** 400,000 JS objects, each running a `setInterval` heartbeat every 250ms, storing waiting jobs as closures in RAM. Result: OOM crashes and uncollectable memory overhead.
*   **BetterQueue's approach:** **zero per-limiter timers**. All queue state (waiting jobs, running counters, token buckets) lives in Redis. A single global "Worker Pool" pulls work using blocking Redis commands (`BRPOP`). If a scope is idle, its RAM and CPU footprint is exactly zero.

---

## 2. Redis Key Structure (The Source of Truth)

Each active queue (e.g., `scope_123`) has a unique `id`. In Redis, this expands into several keys.

### Volatile Runtime Keys (Deleted immediately when the queue empties)
*   **`bq:{id}:jobs` (ZSET)**: Ready jobs. Score = `priority × 1e12 + arrivalSeq`.
*   **`bq:{id}:delayed` (ZSET)**: Jobs not yet due (`scheduleAt` / retry backoff). Score = due time in ms.
*   **`bq:{id}:running` (SET)**: IDs of jobs currently executing. Used to enforce `maxConcurrent`.
*   **`bq:{id}:inflight` (HASH)**: Crash recovery bookmark. JobId → payload.
*   **`bq:{id}:lastRun` (STRING)**: Timestamp of the last time a job was dequeued. Used to enforce `minTime`.
*   **`bq:{id}:reservoir` / `:lastRefresh` / `:lastIncrease` (STRING)**: Token-bucket state.

### Persistent Keys (Auto-expire after the configured TTL)
*   **`bq:{id}:meta` (HASH)**: The configuration for this queue (maxConcurrent, minTime, reservoir, …) plus the monotonic `seq` arrival counter used to break priority ties. Survives when the queue empties so cold-starts are fast.

### Global Keys
*   **`bq:workers:notify:<poolId>` (LIST)**: Per-pool worker signal channel. Each WorkerPool `BRPOP`s only its own channel, so a notification can never be consumed by a different process.
*   **`bq:global:wakeup` (ZSET)**: The central waiting room for rate-limited queues. Member = `<poolId>|<queueId>`, score = the time the queue becomes eligible.
*   **`bq:pools` (ZSET)**: Live pool registry, score = last heartbeat timestamp. Self-cleaning: pools that stop heartbeating fall out of the live window.

---

## 3. The Codebase Step-by-Step

### `LimiterManager.ts` (The Drop-in API Wrapper)
This is what your bot interacts with. It was designed to have the same API as a Bottleneck-based manager so you don't have to rewrite your bot logic.
*   **`registerFeatureConfig()`**: Stores your default rate limits (e.g. `maxConcurrent: 1`, `minTime: 1000`) in an LRU map.
*   **`getLimiter()`**: Checks a local `LRUCache`. If not found, instantiates a lightweight `DistributedLimiter`.
*   **`LimiterWrapper`**: Exposes the `.schedule()` method. It wraps your job in a `Promise.race` to enforce the `timeout` parameter client-side.

### `DistributedLimiter.ts` (The Redis Delegate)
A very thin class. It doesn't hold timers or job arrays.
When you call `limiter.schedule(fn)`, it:
1. Generates a unique job ID.
2. Puts the `fn` closure into the WorkerPool's `pendingJobs` Map in memory (necessary because Redis can't store JS functions).
3. Calls the `bqEnqueue` Lua script to queue the job.

### `WorkerPool.ts` (The Engine)
This is where the magic happens. You instantiate ONE of these per Node.js process.
*   **The Workers**: An array of async infinite loops. Each loop does exactly one thing: `await client.brpop(KEYS.notifyPool(poolId))`. When a queue ID pops out, the worker delegates to the `bqDequeue` Lua script. If successful, it pulls the `fn` closure from `pendingJobs` and executes it.
*   **The Ticker (`startTicker`)**: A single `setInterval` running every 50ms. It runs the `bqWakeupCheck` Lua script to see if any rate-limited queues in `bq:global:wakeup` are ready to run, and refreshes this pool's liveness heartbeat.
*   **Error Handling**: If a job throws, it checks `retryCount`. If retries remain, it recalculates an exponential backoff time and re-enqueues the job into the delayed set. If out of retries, it rejects the promise with a `JobMaxRetriesError`.

### `RedisClient.ts`
Registers all custom Lua scripts using `ioredis.defineCommand`. This guarantees TypeScript type safety for the specific Lua arguments.

---

## 4. The Lua Scripts (The Brains)

Redis Lua scripts are **atomic**. While one runs, no other client can interfere. This prevents race conditions when 10 workers try to grab jobs at the exact same millisecond.

#### `enqueue.lua`
1. Uses `TIME` to get the server time (immune to client clock skew).
2. If the job is not due yet → `ZADD`s it to `bq:{id}:delayed` scored by its due time, and parks the queue in `bq:global:wakeup`.
3. Otherwise → `ZADD`s it to `bq:{id}:jobs` with score `priority × 1e12 + seq`. The `seq` counter lives in the meta hash and makes the score exact, so priority always wins and arrival order breaks ties.
4. Refreshes the `EXPIRE` TTLs on the queue so it stays alive.
5. `LPUSH`es the queue ID onto the **owning pool's** channel (`bq:workers:notify:<ownerId>`) so exactly the process holding the closure is woken up.

#### `dequeue.lua` (The Gatekeeper)
Called by a worker when it wakes up. It decides if a job is *allowed* to run.
1. **Promotes due jobs**: moves any `delayed` job whose due time has passed into the ready set, atomically.
2. **Reservoir maintenance**: lazily applies a refresh (reset) or an increase (add) if the interval has elapsed — no timer required.
3. **Ownership**: scans the ready set for the first job **owned by this pool**. A job's function is a JS closure and only the scheduling process can run it.
4. **Concurrency Check**: `SCARD running` → is it ≥ `maxConcurrent`? If yes, return `concurrent_full`. If every running job belongs to *this* pool, `nextEligibleMs` is `0` (this pool is notified on completion and does not need to poll); if any slot belongs to another process, `nextEligibleMs` is `now + 50` so the pool is woken up again shortly.
5. **MinTime Check**: `GET lastRun` → is `now - lastRun < minTime`? If yes, return `rate_limited` with the exact time the next job may run.
6. **Reservoir Check**: are there tokens left? If 0, return `rate_limited` with the next token time.
7. **Execution**: if all checks pass — `ZREM` the job, `SADD` it to `running`, `HSET` it into `inflight`, update `lastRun`, and return the payload.

#### `complete.lua` (The Cleanup Crew)
Called when a worker finishes a job.
1. `SREM` the job from the `running` SET.
2. `HDEL` the job from the `inflight` HASH.
3. If jobs remain → `LPUSH` a notification to the owning pool and refresh the TTLs. On a concurrency-limited queue it also fans the notification out to the other live pools, so a job waiting on a slot held by *another* process starts as soon as the slot is released.
4. If the queue is completely empty → immediately `DEL` the volatile keys (jobs, delayed, running, inflight, lastRun) and remove the wakeup entry. The `meta` key survives with its TTL so cold-starts stay cheap.

#### `wakeup_check.lua`
Called by the global ticker every 50ms.
1. `ZRANGEBYSCORE wakeup -inf <now>` → queues that were rate-limited but are now eligible.
2. Removes exactly the entries it processed (so entries beyond the batch size are not lost).
3. `LPUSH`es them onto the owning pool's channel.

#### `recover.lua`
Called once per queue when a limiter is first used.
1. Scans the `inflight` HASH for jobs older than `visibilityTimeoutMs` — these were orphaned by a crash.
2. Removes them from `running` and re-queues them in the ready set with an incremented retry counter (if the retry budget allows).
3. Notifies the owning pool.

#### `stats.lua`
Atomic stats snapshot (`queueSize`, `runningCount`, `reservoir`). It also performs the same lazy reservoir maintenance as `dequeue.lua`, so the reported token count is accurate even when the queue is idle.

#### `pool_heartbeat.lua` / `pool_unregister.lua`
Register/refresh a pool in the `bq:pools` registry (dropping pools that stopped heartbeating), and deregister + delete the pool's channel on graceful shutdown.

---

## 5. Known Issues (Open Bugs)

### 5.1 ~~Cross-process jobs can stall when the concurrency slot is held by another process~~ — FIXED

**Status:** fixed.

**What the bug was.** When two processes shared a queue `id` and the
concurrency slot was occupied by a job belonging to **another** process, the
waiting process's job sat in Redis indefinitely — until an unrelated
notification happened to poke the queue.

**Why it happened.** `dequeue.lua` checked `maxConcurrent` *before* it looked
at whether the queue had any work this pool could run:

```lua
if maxConcurrent > 0 then
  local running = tonumber(redis.call('SCARD', runningKey))
  if running >= maxConcurrent then
    return { 'concurrent_full', '', '0' }
  end
end
```

It returned status `concurrent_full` with `nextEligibleMs = '0'` — i.e. "no
idea when to retry". `src/WorkerPool.ts` then only scheduled a wakeup for
three of the five dequeue statuses (`rate_limited`, `empty`, `blocked`), so
`concurrent_full` never produced a wakeup and the job was never re-notified.

**How it was fixed.** Two changes:

1. **`dequeue.lua` reordered.** The owner-aware candidate scan (`ZRANGE` +
   `firstOwned`) now runs *before* the concurrency check, so the check
   applies to a job this pool is actually about to take. When every slot is
   taken, the script inspects the running set:

   - if **all** running jobs belong to *this* pool → `nextEligibleMs = 0`.
     That pool is notified directly by `complete.lua` when the job finishes,
     so it does not need to poll at all (the single-process path is
     unchanged).
   - if **any** slot belongs to *another* process → `nextEligibleMs =
     now + 50`. The pool cannot know when the slot will be released, so it
     asks to be woken up shortly.

2. **`WorkerPool.drainQueue()` now schedules a wakeup for
   `concurrent_full`** as well, so the retry time that `dequeue.lua` reports
   is actually acted on.

As a bonus, `complete.lua` now fans a notification out to the other live
pools when a concurrency slot is released, so the waiting job usually starts
on the very next tick rather than on the next poll.

The wakeup ZSET is keyed by `"<poolId>|<queueId>"` and scheduled through
`wakeup_schedule.lua`, which keeps the *earliest* wakeup atomically. A queue
therefore never holds more than one pending wakeup per pool — the ZSET stays
bounded no matter how often a worker retries.

`test/cross-process.test.ts` is the regression test: it spawns a real second
Node process and asserts the waiting job starts within a bounded time after
the slot is released.

### 5.2 Redis Cluster is not supported

The Lua scripts operate on multiple keys at once and the key names carry no
Redis Cluster hash tag, so `CROSSSLOT` errors occur on a clustered
deployment. This affects Redis Cluster and ElastiCache in cluster mode. Use a
single-node Redis or a Sentinel-managed primary.

A hash tag in the queue id does put every key of that queue on one slot
(e.g. an id of `{tenant-42}:export` produces `bq:{tenant-42}:export:meta`),
but the global keys `bq:global:wakeup` and `bq:pools` are shared across all
queues and would still need special handling.

---

## 6. Day in the Life of a Rate-Limited Job

**Scenario:** Scope 123 has `maxConcurrent: 1`, `minTime: 1000`. Two members join at the exact same time.

**T=0ms (Member 1 & 2 Join)**
* Your bot calls `.schedule()` twice.
* `enqueue.lua` runs twice. Both jobs are parked in the `bq:scope:123:jobs` ZSET with priority-scored values.
* Two notifications are pushed to this pool's channel `bq:workers:notify:<poolId>`.

**T=5ms (Worker A Wakes Up)**
* Worker A pops "scope:123" from its channel.
* Runs `dequeue.lua`. `running` is 0. Job 1 is allowed.
* Job 1 is moved to the `running` SET and `inflight` HASH.
* Node.js starts executing Job 1's function.

**T=10ms (Worker B Wakes Up)**
* Worker B pops the second "scope:123" notification.
* Runs `dequeue.lua`. `running` is 1. `maxConcurrent` is 1.
* **Denied.** The script returns `concurrent_full`. The running job belongs to this same pool, so `nextEligibleMs` is `0` — no wakeup is needed, because `complete.lua` will notify this pool directly when Job 1 finishes.
* Worker B goes back to sleep. Job 2 stays in the Redis ZSET.

**T=150ms (Job 1 Completes)**
* The upstream API responds. Job 1 is done.
* `complete.lua` runs. Removes Job 1 from the `running` SET.
* Jobs remain → a new notification is pushed to the owning pool.

**T=155ms (Worker Wakes Up)**
* Runs `dequeue.lua`. Concurrency is now 0 (Job 1 finished).
* **BUT**, `lastRun` was T=5ms and `minTime` is 1000ms. 150ms < 1000ms.
* **Denied.** The script returns `rate_limited` and the exact time Job 2 *should* run: T=1005ms.

**T=160ms (Worker parks the queue)**
* The worker schedules a wakeup: `zadd bq:global:wakeup 1005 <poolId>|scope:123`.

**T=1000ms (The Ticker)**
* The 50ms ticker checks `bq:global:wakeup`. 1000 >= 1005? No. Nothing happens.

**T=1050ms (The Ticker)**
* Ticker checks. 1050 >= 1005? Yes!
* `wakeup_check.lua` pulls the entry out and pushes "scope:123" onto the owning pool's channel.

**T=1055ms (Worker Wakes Up)**
* Runs `dequeue.lua`. Concurrency is 0. Time since `lastRun` is > 1000ms.
* **Allowed.** Job 2 executes.

**T=1200ms (Job 2 Completes)**
* `complete.lua` runs. No more jobs. Nothing running. The queue is empty.
* **Immediate Cleanup**: the volatile running, inflight, jobs and delayed keys are instantly deleted. The meta config key keeps its TTL and is left alone safely.
