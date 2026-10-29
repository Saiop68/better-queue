# BetterQueue

> A Redis-backed distributed rate limiter and job queue for Node.js.
> One global timer instead of N `setInterval`s. Real cross-process priority.

[![TypeScript](https://img.shields.io/badge/TypeScript-5.x-blue.svg)](https://www.typescriptlang.org/)
[![Redis](https://img.shields.io/badge/Redis-6%2B-red.svg)](https://redis.io/)
[![ioredis](https://img.shields.io/badge/ioredis-5.x-orange.svg)](https://github.com/luin/ioredis)

---

## Better than Bottleneck

Bottleneck is a solid, battle-tested library. BetterQueue is a drop-in-shaped
alternative for the case where you have **many** limiters or **many**
processes. The differences below are the ones that actually matter, and each
one is implemented and covered by the test suite.

| | Bottleneck | BetterQueue |
|---|---|---|
| Timers | one heartbeat per limiter | **one global ticker for the whole process** |
| Queue contents | local to each limiter; a cluster does not track them | **in Redis — shared by every process** |
| Priority across processes | *"Priorities are local"* (Bottleneck's own docs) | **Redis-backed: a priority-1 job outranks a priority-9 job in any process** |
| Queue state after a crash | lost | **recoverable** (inflight jobs are re-queued) |
| JS heap per limiter | grows with the limiter's job queue | **fixed** — the queue lives in Redis |
| Delayed jobs (`scheduleAt`) | not supported | **supported** (`scheduleAt`) |
| Token bucket "increase" mode | `reservoirIncreaseInterval` | **supported**, plus a `reservoirIncreaseMaximum` cap |

### The four concrete advantages

**1. Cross-process priority that actually works.**

Bottleneck's README states plainly: *"Priorities are local. A higher priority
job will run before a lower priority job on the same limiter. Another limiter
on the cluster might run a lower priority job before our higher priority
one."* It also notes that queued jobs *"are NOT stored on Redis. They are
local to each limiter."*

BetterQueue stores the queue in Redis and composes each job's ZSET score as
`priority × 1e12 + arrivalSeq`. Priority occupies the high-order digits, so
ordering is global and deterministic:

```
priority 1  → score 1000000000002
priority 9  → score 9000000000003
```

Two processes writing to the same queue id produce one ordering, not two.

**2. One global ticker instead of N timers.**

Bottleneck creates a heartbeat per limiter. At 100k limiters that is 100k
`setInterval` handles. BetterQueue runs **one** `setInterval` per process
(`tickIntervalMs`, default 50 ms) that drains a single global wakeup ZSET and
re-notifies the workers whose queues became eligible. Per-queue limiters hold
configuration only.

**3. The queue survives a crash.**

Jobs are tracked in a Redis `inflight` HASH. On restart, `recoverQueue()`
re-queues jobs older than `visibilityTimeoutMs` with an incremented retry
counter. Bottleneck keeps its queue in memory, so a crash loses it.

**4. Delayed jobs are first-class.**

`scheduleAt` puts a job in a `delayed` ZSET scored by its due time. It is
promoted to the ready set atomically, at the exact moment it becomes due,
using the Redis server clock — so client clock skew cannot make a job fire
early, late, or never.

---

## Installation

```bash
npm install better-queue ioredis
```

Requires Node.js ≥ 18 and Redis ≥ 6. Runtime dependencies are `ioredis` and
`lru-cache` — nothing else.

---

## Quick Start

```ts
import { WorkerPool, DistributedLimiter } from 'better-queue';

// 1. Start the global pool ONCE (e.g. in your app's startup)
const pool = await WorkerPool.create({
  redis: { host: 'localhost', port: 6379 },
  workers: 8,
});

// 2. Create a limiter for each queue (e.g. per guild)
const limiter = new DistributedLimiter({
  id: 'guild:123456789',
  maxConcurrent: 2,
  minTime: 500,
  reservoir: 10,
  reservoirRefreshInterval: 1_000,
  reservoirRefreshAmount: 5,
});

// 3. Schedule work
const result = await limiter.schedule(() => someAsyncTask());
```

---

## API

### `WorkerPool.create(options)`

Initialize the global worker pool. Call once at startup.

```ts
const pool = await WorkerPool.create({
  redis: { host: 'localhost', port: 6379 },  // ioredis options
  workers: 8,                 // concurrent workers (default: 64)
  tickIntervalMs: 50,         // global ticker interval (default: 50ms)
  visibilityTimeoutMs: 300_000, // crash recovery timeout (default: 5min)
  pendingJobTimeoutMs: 300_000, // orphan sweeper timeout (default: 5min, 0 = off)
});
```

### `new DistributedLimiter(options)`

Create a rate-limited queue. Instantiation is cheap — the instance holds
configuration only; the queue itself lives in Redis.

```ts
const limiter = new DistributedLimiter({
  id: 'guild:123456789',           // required — unique queue key
  maxConcurrent: 2,                // max parallel jobs (default: 1)
  minTime: 500,                    // ms between job starts (default: 0)
  reservoir: 10,                   // initial token count (default: unlimited)
  reservoirRefreshInterval: 1_000, // token refill interval ms (default: 0)
  reservoirRefreshAmount: 5,       // tokens RESET to on each refill (default: 0)
  reservoirIncreaseInterval: 0,    // token INCREASE interval ms (default: 0)
  reservoirIncreaseAmount: 0,      // tokens ADDED on each increase (default: 0)
  reservoirIncreaseMaximum: 0,     // cap for increase mode (default: 0 = unbounded)
  priority: 5,                     // default priority 1–10 (default: 5)
  ttl: 86_400,                     // Redis key TTL seconds (default: 3600)
  retryOnFailure: true,            // retry failed jobs (default: true)
  maxRetries: 3,                   // max retry attempts (default: 3)
});
```

#### Reservoir: refresh vs. increase

Two different token-bucket behaviours, matching Bottleneck's naming:

- **Refresh** (`reservoirRefreshInterval` + `reservoirRefreshAmount`) —
  every interval the bucket is **reset** to `reservoirRefreshAmount`.
  Use it for a fixed quota per window ("100 requests per minute").
- **Increase** (`reservoirIncreaseInterval` + `reservoirIncreaseAmount`) —
  every interval the bucket **gains** `reservoirIncreaseAmount` tokens.
  Use it for a quota that trickles back ("40 now, +2 every second").
  `reservoirIncreaseMaximum` caps the bucket.

Refresh and increase are mutually exclusive: if both are configured, the
refresh (reset) wins.

### `limiter.schedule(fn, options?)`

Enqueue an async function. Returns a Promise that resolves with `fn`'s return
value.

```ts
const data = await limiter.schedule(
  () => fetch('/api/endpoint').then(r => r.json()),
  {
    priority: 1,                   // override priority (1=highest, 10=lowest)
    scheduleAt: Date.now() + 5_000, // delay job by 5 seconds
    maxRetries: 5,                 // override max retries for this job
  },
);
```

Bottleneck's argument order (`schedule(options, fn)`) is also supported, so
either style compiles:

```ts
await limiter.schedule({ priority: 1 }, () => doWork());
```

### Observability

```ts
// All stats come from Redis — never from memory
await limiter.getStats();        // { queueSize, runningCount, reservoir }
await limiter.getQueueSize();    // number of pending jobs (ready + delayed)
await limiter.getRunningCount(); // number of executing jobs
await limiter.getReservoir();    // current token count (null if not configured)
```

### Lifecycle

```ts
await limiter.updateSettings({ minTime: 100, reservoir: 20 }); // hot-reload config
await limiter.drain();  // wait for all queued + running jobs to finish
limiter.stop();         // no longer accept new jobs (non-blocking)
await pool.stop();      // gracefully stop all workers
```

### Events

```ts
limiter.on('executing', (jobId) => console.log('started:', jobId));
limiter.on('done',      (jobId) => console.log('done:', jobId));
limiter.on('failed',    (jobId, err) => console.error('failed:', jobId, err));
limiter.on('retry',     (jobId, attempt) => console.log('retry:', jobId, attempt));
limiter.on('drained',   () => console.log('queue empty'));
```

Events are scoped to the limiter's own queue id.

---

## Multiple processes

Each process creates its own `WorkerPool`. Jobs are stamped with the id of
the pool that scheduled them, because a job's executable function is a JS
closure that only exists in the scheduling process. Each pool therefore
listens on its **own** notification channel (`bq:workers:notify:<poolId>`),
so a notification can never be consumed by a different process.

Rate-limit accounting (`maxConcurrent`, `minTime`, the reservoir) is
**global** — it is shared by every process working on the same queue id,
which is what makes the limiter meaningful across a cluster.

Pools announce themselves in a `bq:pools` registry that is refreshed by the
global ticker and self-cleans when a process dies.

---

## Redis Schema

| Key | Type | Purpose |
|-----|------|---------|
| `bq:{id}:jobs` | ZSET | Ready jobs. Score = `priority × 1e12 + arrivalSeq` |
| `bq:{id}:delayed` | ZSET | Jobs not yet due. Score = due time (ms) |
| `bq:{id}:running` | SET | Job IDs currently executing |
| `bq:{id}:inflight` | HASH | `jobId → payload` for crash recovery |
| `bq:{id}:meta` | HASH | Queue config + the arrival counter |
| `bq:{id}:reservoir` | STRING | Current token count |
| `bq:{id}:lastRun` | STRING | Timestamp of last dequeue (ms) |
| `bq:{id}:lastRefresh` | STRING | Timestamp of last reservoir refresh (ms) |
| `bq:{id}:lastIncrease` | STRING | Timestamp of last reservoir increase (ms) |
| `bq:workers:notify:<poolId>` | LIST | Per-pool worker signal channel |
| `bq:global:wakeup` | ZSET | Scheduled wakeups, member = `<poolId>|<queueId>` |
| `bq:pools` | ZSET | Live pool registry, score = last heartbeat (ms) |

All keys use a configurable TTL. Inactive queues auto-expire — no manual
cleanup needed.

---

## Memory Architecture

### Why Bottleneck grows at scale

When you create 10,000 Bottleneck instances (one per guild), you get:
- 10,000 `setInterval` handles (one heartbeat per limiter)
- 10,000 in-memory job queues (grows with pending jobs)
- Job history retained in memory (grows over time)

### How BetterQueue differs

```
DistributedLimiter instance:       WorkerPool (global, one per process):
  id: "guild:123"         (28B)      pendingJobs Map:
  opts: {...config...}    (40B)        "uuid-1" → {fn, resolve, reject}  ← exists only while job pending
  pool: ref               (8B)         "uuid-2" → ...
  ─────────────────────────────        (auto-deleted on completion)
  Total: ~50 bytes

  100,000 limiters = ~5 MB heap
```

**The `pendingJobs` Map** is the only mutable in-process state. Its size
equals the number of jobs that have been `schedule()`d but not yet completed —
bounded by your application's concurrency, not by the number of limiters.

---

## Crash Recovery

If your process crashes while jobs are executing:

1. On restart, `recoverQueue()` runs automatically on each limiter's first use.
2. It scans the `bq:{id}:inflight` HASH for jobs older than
   `visibilityTimeoutMs` (5 min).
3. Eligible jobs are re-enqueued in the ZSET with an incremented retry counter.

> **Note:** the job's closure lived in the crashed process, so a recovered job
> cannot be executed — it preserves the queue's ordering and counters, but the
> work itself must be re-submitted by your application. The original Promise
> is gone either way.

---

## Lua Scripts

All critical operations are implemented as atomic Lua scripts to prevent race
conditions:

| Script | Purpose |
|--------|---------|
| `enqueue.lua` | Add job to the ready/delayed set + signal the owner pool |
| `dequeue.lua` | Fetch the next eligible job owned by this pool (rate-limit-aware, lazy reservoir refresh/increase) |
| `complete.lua` | Release the running slot + re-signal + idle cleanup |
| `wakeup_check.lua` | Global ticker — wake queues whose delay expired |
| `recover.lua` | Re-enqueue orphaned crashed jobs |
| `stats.lua` | Atomic stats snapshot (with lazy reservoir maintenance) |
| `pool_heartbeat.lua` | Register/refresh this pool's liveness |
| `pool_unregister.lua` | Deregister a pool on shutdown |

---

## Bottleneck compatibility layer

`src/LimiterManager.ts` provides a drop-in-shaped manager for code that was
written against a Bottleneck-based `LimiterManager`:

```ts
import limiterManager from 'better-queue/LimiterManager';

limiterManager.registerFeatureConfig('guild-member-add', {
  maxConcurrent: 1,
  reservoir: 10,
  reservoirRefreshAmount: 10,
  reservoirRefreshInterval: 10_000,
  minTime: 1_000,
  timeout: 3_600_000,
});

const limiter = limiterManager.getLimiter(guildId, 'guild-member-add');
await limiter.schedule({ id: 'some-id' }, () => doWork());
```

Both `better-queue` and `better-queue/LimiterManager` are valid import paths.

> The Bottleneck `Group`/`Cluster` API is **not** reimplemented — BetterQueue
> achieves the same effect by keying queues in Redis, which is a different
> (and simpler) model.

---

## Testing

```bash
npm test
```

The suite runs against a real Redis instance (port `6399` by default — see
`test/helpers.ts`) and covers basic scheduling, `maxConcurrent`, `minTime`,
reservoir refresh, reservoir increase, `scheduleAt`, priority ordering,
error + retry, stats and `drain()`.

---

## Performance

See `examples/benchmark.ts`. Expect:

- **~50k–100k jobs/sec** throughput with no rate limits (32 concurrent, fast fns)
- **~50 bytes** JS heap per limiter instance
- **Flat heap** over millions of jobs (no accumulation)
- **1 timer** for any number of limiters

---

## License

MIT
