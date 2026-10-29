# BetterQueue: Complete Architecture & Codebase Guide

BetterQueue is a high-performance, distributed rate limiter and job queue designed to replace `Bottleneck` in large-scale Node.js applications.

It solves two structural problems with Bottleneck at scale: **memory growth from per-queue state** and **CPU starvation from tens of thousands of idle `setInterval` timers**.

---

## 1. The Core Philosophy

In a typical 10,000 guild bot with 40 features per guild, you might have **400,000 potential rate limiters**.

*   **Bottleneck's approach:** 400,000 JS objects, each running a `setInterval` heartbeat every 250ms, storing waiting jobs as closures in RAM. Result: OOM crashes and uncollectable memory overhead.
*   **BetterQueue's approach:** **zero per-limiter timers**. All queue state (waiting jobs, running counters, token buckets) lives in Redis. A single global "Worker Pool" pulls work using blocking Redis commands (`BRPOP`). If a guild is idle, its RAM and CPU footprint is exactly zero.

---

## 2. Redis Key Structure (The Source of Truth)

Each active queue (e.g., `guild_123`) has a unique `id`. In Redis, this expands into several keys.

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
3. **Concurrency Check**: `SCARD running` → is it ≥ `maxConcurrent`? If yes, return `concurrent_full`.
4. **MinTime Check**: `GET lastRun` → is `now - lastRun < minTime`? If yes, return `rate_limited` with the exact time the next job may run.
5. **Reservoir Check**: are there tokens left? If 0, return `rate_limited` with the next token time.
6. **Ownership**: scans the ready set for the first job **owned by this pool**. A job's function is a JS closure and only the scheduling process can run it.
7. **Execution**: if all checks pass — `ZREM` the job, `SADD` it to `running`, `HSET` it into `inflight`, update `lastRun`, and return the payload.

#### `complete.lua` (The Cleanup Crew)
Called when a worker finishes a job.
1. `SREM` the job from the `running` SET.
2. `HDEL` the job from the `inflight` HASH.
3. If jobs remain → `LPUSH` a notification to the owning pool and refresh the TTLs.
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

### 5.1 Cross-process jobs can stall when the concurrency slot is held by another process

**Status:** known, open, reproducible. **Not fixed** in the current release.

**Symptom.** When two processes share a queue `id` and the queue's concurrency
slot is occupied by a job belonging to **another** process, the waiting
process's job can sit in Redis for a long time — until an unrelated
notification happens to poke the queue.

**Reproduction** (two Node processes, same queue id, `maxConcurrent: 1`):

1. Process A schedules a job that holds the slot for 2 seconds.
2. Process B schedules a priority-1 job 900 ms later.
3. Process A observes its own queue while B's job is pending:

   ```
   [a] t+2s  pending=1  running=0  wakeup=0
   [a] t+4s  pending=1  running=0  wakeup=0
   [a] t+6s  pending=1  running=0  wakeup=0
   [a] t+8s  pending=1  running=0  wakeup=0
   [a] t+10s pending=1  running=0  wakeup=0
   ```

4. B's job does not run. After manually `LPUSH`ing a notification onto the
   pool's notify channel, it is released immediately:

   ```
   [a] poked notify channel of 997231-6d414398
   [a] after poke: pending= 0 running= 0
   ```

5. B eventually resolves — it is a **stall, not a permanent loss**:

   ```
   [b] B-prio1 RAN after 10108 ms
   [b] outcome: RESOLVED
   ```

**Root cause.** Two places conspire.

`scripts/dequeue.lua` checks concurrency *before* it looks at whether the
queue has any work this pool could run:

```lua
if maxConcurrent > 0 then
  local running = tonumber(redis.call('SCARD', runningKey))
  if running >= maxConcurrent then
    return { 'concurrent_full', '', '0' }
  end
end
```

It returns status `concurrent_full` with `nextEligibleMs = '0'` — i.e. "no
idea when to retry".

`src/WorkerPool.ts` then only schedules a wakeup for three of the five
dequeue statuses:

```ts
if (
  (status === 'rate_limited' || status === 'empty' || status === 'blocked') &&
  nextEligibleMs > 0
) {
  await this.scheduleWakeup(queueId, nextEligibleMs);
}
```

`concurrent_full` is not in that list, and its `nextEligibleMs` is `0`
anyway. So no wakeup is ever scheduled for this queue, and the job sits in
the ready ZSET until some *other* event pushes a notification onto the
pool's channel.

The `blocked` status — which does carry a retry time (`now + 50`) and would
have scheduled a wakeup — is only reached **after** the `concurrent_full`
early-return, so it never fires for this case.

**Impact.** Jobs in the affected queue wait indefinitely instead of for a
bounded time. In the reproduction, a job that should have started as soon as
the slot freed (t≈2 s) started at t≈10 s, when an unrelated notification
arrived.

**Workaround until fixed:**

- Prefer **one process per queue id** — the bug only appears when the
  concurrency slot is held by a different process.
- Or use a large enough `maxConcurrent` that a slot is essentially always
  available, and rely on `minTime` plus the reservoir for pacing (those
  paths *do* schedule wakeups correctly).
- Or monitor `getQueueSize()` and nudge the queue if it stays non-zero.

**Recommended fix (not applied here).** Either give `concurrent_full` a
sensible `nextEligibleMs` and include it in the wakeup-scheduling condition in
`drainQueue()`, or reorder `dequeue.lua` so the owner-aware candidate scan
runs before the concurrency check and the `blocked` status (which already
carries a retry time) is returned when the slot is held by another pool.

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

**Scenario:** Guild 123 has `maxConcurrent: 1`, `minTime: 1000`. Two members join at the exact same time.

**T=0ms (Member 1 & 2 Join)**
* Your bot calls `.schedule()` twice.
* `enqueue.lua` runs twice. Both jobs are parked in the `bq:guild:123:jobs` ZSET with priority-scored values.
* Two notifications are pushed to this pool's channel `bq:workers:notify:<poolId>`.

**T=5ms (Worker A Wakes Up)**
* Worker A pops "guild:123" from its channel.
* Runs `dequeue.lua`. `running` is 0. Job 1 is allowed.
* Job 1 is moved to the `running` SET and `inflight` HASH.
* Node.js starts executing Job 1's function.

**T=10ms (Worker B Wakes Up)**
* Worker B pops the second "guild:123" notification.
* Runs `dequeue.lua`. `running` is 1. `maxConcurrent` is 1.
* **Denied.** The script returns `concurrent_full`.
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
* The worker schedules a wakeup: `zadd bq:global:wakeup 1005 <poolId>|guild:123`.

**T=1000ms (The Ticker)**
* The 50ms ticker checks `bq:global:wakeup`. 1000 >= 1005? No. Nothing happens.

**T=1050ms (The Ticker)**
* Ticker checks. 1050 >= 1005? Yes!
* `wakeup_check.lua` pulls the entry out and pushes "guild:123" onto the owning pool's channel.

**T=1055ms (Worker Wakes Up)**
* Runs `dequeue.lua`. Concurrency is 0. Time since `lastRun` is > 1000ms.
* **Allowed.** Job 2 executes.

**T=1200ms (Job 2 Completes)**
* `complete.lua` runs. No more jobs. Nothing running. The queue is empty.
* **Immediate Cleanup**: the volatile running, inflight, jobs and delayed keys are instantly deleted. The meta config key keeps its TTL and is left alone safely.
