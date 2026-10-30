# BetterQueue

> **Redis-backed distributed rate limiting with cross-process priority, crash recovery and one global ticker for the whole process.**
>
> A rate limiter and job queue for Node.js whose entire queue state lives in
> Redis — so many queues and many processes cost little memory, and priority
> ordering holds across the whole fleet.

[![npm version](https://img.shields.io/npm/v/@destroyer777777%2Fbetterqueue.svg)](https://www.npmjs.com/package/@destroyer777777/betterqueue)
[![npm downloads](https://img.shields.io/npm/dm/@destroyer777777%2Fbetterqueue.svg)](https://www.npmjs.com/package/@destroyer777777/betterqueue)
[![CI](https://github.com/Saiop68/better-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/Saiop68/better-queue/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![node](https://img.shields.io/badge/node-%3E%3D18.0.0-brightgreen.svg)](https://nodejs.org)
[![typescript](https://img.shields.io/badge/TypeScript-5.x-blue.svg)](https://www.typescriptlang.org/)
[![redis](https://img.shields.io/badge/Redis-6%2B-red.svg)](https://redis.io/)
[![ioredis](https://img.shields.io/badge/ioredis-5.x-orange.svg)](https://github.com/luin/ioredis)
[![website](https://img.shields.io/badge/website-saiop68.github.io%2Fbetter--queue-blue.svg)](https://saiop68.github.io/better-queue/)

**[Live demo & benchmarks →](https://saiop68.github.io/better-queue/)**

```bash
npm install @destroyer777777/betterqueue ioredis
```

---

## Table of contents

- [Why not Bottleneck?](#why-not-bottleneck)
  - [Measured](#measured)
  - [The four concrete advantages](#the-four-concrete-advantages)
- [Redis deployment modes](#redis-deployment-modes)
  - [Single node (default)](#single-node-default)
  - [Sentinel (automatic failover)](#sentinel-automatic-failover)
  - [Redis Cluster (not supported yet)](#redis-cluster-not-supported-yet)
- [Installation](#installation)
- [Getting started](#getting-started)
- [Core concepts](#core-concepts)
  - [Queues and limiters](#queues-and-limiters)
  - [The reservoir (token bucket)](#the-reservoir-token-bucket)
  - [`maxConcurrent` vs `minTime`](#maxconcurrent-vs-mintime)
  - [Priority and the job score](#priority-and-the-job-score)
  - [The two-ZSET design](#the-two-zset-design)
- [Multiple processes and distributed usage](#multiple-processes-and-distributed-usage)
- [Redis schema](#redis-schema)
- [API reference](#api-reference)
  - [`WorkerPool.create(options)`](#workerpoolcreateoptions)
  - [`new DistributedLimiter(options)`](#new-distributedlimiteroptions)
  - [`limiter.schedule(fn, options?)`](#limiterschedulefn-options)
  - [Observability](#observability)
  - [Lifecycle](#lifecycle)
  - [Events](#events)
  - [Errors](#errors)
- [Full options reference](docs/OPTIONS.md)
  - [`WorkerPool.create()` options](#workerpoolcreate-options)
  - [`DistributedLimiter` options](#distributedlimiter-options)
  - [`schedule()` options](#schedule-options)
  - [`updateSettings()` patchable fields](#updatesettings-patchable-fields)
  - [`LimiterManager` / `FeatureConfig` options](#limitermanager--featureconfig-options)
  - [`LimiterWrapper` / `ScheduleJobOptions`](#limiterwrapper--schedulejoboptions)
- [Crash recovery](#crash-recovery)
- [Bottleneck compatibility layer](#bottleneck-compatibility-layer)
- [Lua scripts](#lua-scripts)
- [Performance](#performance)
- [Known issues and limitations](#known-issues-and-limitations)
- [Troubleshooting](#troubleshooting)
- [Requirements](#requirements)
- [Testing](#testing)
- [License](#license)
- [Contributing](#contributing)

---

## Why not Bottleneck?

Bottleneck is a solid, battle-tested library, and this project does not try to
replace it for everybody. It is aimed at the specific case where you have
**many limiters**, **many processes**, or both — the situation where
Bottleneck's in-process design starts to cost you memory and CPU.

The comparison below is based on Bottleneck's own documentation and source.

| | Bottleneck | BetterQueue |
|---|---|---|
| Timers | one `setInterval` heartbeat **per limiter that uses a reservoir** (`heartbeatInterval`, 250 ms local / 5000 ms clustered) — created only when `reservoirRefreshInterval` or `reservoirIncreaseInterval` is set | **one global ticker per process** (`tickIntervalMs`, default 50 ms) + one orphan sweeper |
| Queue contents | **local to each limiter** — "Queued jobs are NOT stored on Redis" | **in Redis** — shared by every process |
| Priority across processes | **local** — "Priorities are local… Another limiter on the cluster might run a lower priority job before our higher priority one" | **Redis-backed and global** — a priority-1 job outranks a priority-9 job in any process |
| Arrival order across processes | not guaranteed — "Another limiter on the cluster might run a job received later before ours runs" | global, via a server-side monotonic sequence number |
| Queue state after a crash | lost (in-memory) | **recoverable** — inflight jobs are re-queued |
| Delayed jobs (`scheduleAt`) | not supported | **supported** (`scheduleAt`) |
| Token bucket "increase" mode | `reservoirIncreaseInterval` | **supported**, plus a `reservoirIncreaseMaximum` cap |
| JS heap per limiter | grows with the limiter's job queue | bounded by pending jobs, not by limiter count |
| Redis Cluster | supported | **not supported** — see [docs/CLUSTER-SUPPORT.md](docs/CLUSTER-SUPPORT.md) |
| Redis Sentinel | supported | **supported** — `{ sentinels, name }`, automatic failover |


### Measured

Same machine, same Redis, each library in its own process. 1000 limiters,
51,000 jobs. The script that produces these numbers is in
[`benchmark/`](benchmark/) and prints them on every run.

| Metric | Bottleneck 2.19.5 | BetterQueue |
|---|---:|---:|
| RSS after creating 1000 limiters | 9 MB | 3 MB |
| RSS after 51,000 jobs | **415 MB** | **222 MB** |
| Heap after forced GC | 14 MB | 11 MB |
| Throughput | 9,957 jobs/s | 11,456 jobs/s |

Bottleneck keeps its queue contents in the process, so the cost of a limiter
grows with the work queued on it. BetterQueue keeps queue contents in Redis and
leaves a thin handle in the process, which is where the
193 MB difference at 1000 limiters comes from.

Reproduce it:

```bash
npm install bottleneck --no-save
npm run build
node --expose-gc benchmark/compare.mjs bottleneck
node --expose-gc benchmark/compare.mjs betterqueue
```

### The four concrete advantages

**1. Cross-process priority that actually works.**

Bottleneck's README states plainly: *"Priorities are local. A higher priority
job will run before a lower priority job **on the same limiter**. Another
limiter on the cluster might run a lower priority job before our higher
priority one."* It also notes that queued jobs *"are NOT stored on Redis. They
are local to each limiter."*

BetterQueue stores the queue in Redis and composes each job's ZSET score as
`priority × 1e12 + arrivalSeq`. Priority occupies the high-order digits, so
ordering is global and deterministic:

```
priority 1  → score 1000000000002
priority 9  → score 9000000000003
```

Two processes writing to the same queue id produce one ordering, not two.

> **Caveat.** The ordering is genuinely global — the *scores* are computed
> server-side and compared in one shared ZSET. Execution is per-process, but
> the concurrency slots are shared: a job waits until a slot frees up, even
> when the slot is held by a job of another process. See
> [Known issues and limitations](#known-issues-and-limitations).

**2. One global ticker instead of N timers.**

Bottleneck creates a heartbeat `setInterval` for each limiter that uses a
reservoir — it is started in `LocalDatastore._startHeartbeat()` and only when
`reservoirRefreshInterval` or `reservoirIncreaseInterval` is configured. An
application that never uses a reservoir pays nothing here; one that does pays
one timer per limiter.

BetterQueue runs **one** `setInterval` per process (`tickIntervalMs`, default
50 ms) that drains a single global wakeup ZSET and re-notifies the workers
whose queues became eligible, plus one orphan sweeper. Per-queue limiters hold
configuration only.

Measured with `process.getActiveResourcesInfo()` in the
[`benchmark/`](benchmark/) script, 1000 limiters, no reservoir configured:

```
live timers, Bottleneck:  0
live timers, BetterQueue: 0
```

Both are zero here because no limiter used a reservoir. The difference is that
Bottleneck's count scales with the number of reservoir-backed limiters while
BetterQueue's stays at two per process no matter how many queues exist.

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
# npmjs.org — no token needed, this is the main channel
npm install @destroyer777777/betterqueue ioredis

# or GitHub Packages (requires a GitHub token with read:packages)
npm install @destroyer777777/betterqueue ioredis
```

Requires Node.js ≥ 18 and Redis ≥ 6. Runtime dependencies are `ioredis` and
`lru-cache` — nothing else. You pass your own `ioredis` options straight
through to `WorkerPool.create()`, so the connection is yours to configure:

```ts
await WorkerPool.create({
  redis: { host: '127.0.0.1', port: 6379, password: process.env.REDIS_PASSWORD },
});
```

> **On the package name.** The unscoped name `better-queue` is already taken on
> npm by an unrelated package, and npm's similarity policy rejects
> `betterqueue` as well, so this library is published under a scope:
> `@destroyer777777/betterqueue`. The GitHub repository is
> [Saiop68/better-queue](https://github.com/Saiop68/better-queue).

---

## Redis deployment modes

### Single node (default)

```ts
const pool = await WorkerPool.create({
  redis: { host: '127.0.0.1', port: 6379 },
});
```

### Sentinel (automatic failover)

Point the pool at the sentinels and the name the master is registered under.
The Sentinel resolves the current primary and the client follows it through a
failover.

```ts
const pool = await WorkerPool.create({
  redis: {
    sentinels: [
      { host: '10.0.0.1', port: 26379 },
      { host: '10.0.0.2', port: 26379 },
      { host: '10.0.0.3', port: 26379 },
    ],
    name: 'mymaster',
    // optional
    password: process.env.REDIS_PASSWORD,
    sentinelPassword: process.env.SENTINEL_PASSWORD,
  },
});
```

There is no key-layout change and no script change for Sentinel — the queue
state and the atomicity guarantees are identical to a single-node deployment.
When the primary dies, the Sentinel promotes a replica and the pool reconnects
to it.

### Redis Cluster (not supported yet)

The Lua scripts operate on several keys at once, and Redis Cluster requires
every key in a script to land in the same slot. The current key layout has no
hash tag, so a clustered deployment returns `CROSSSLOT`. This was measured
against a live 3-node cluster:

```
current key layout         -> CROSSSLOT Keys in request don't hash to the same slot
all queue keys, no globals -> OK
queue + bq:global:wakeup   -> CROSSSLOT
```

The fix is a key-layout change plus a client-side notify fan-out. The full
analysis, the migration plan and the test plan are in
**[docs/CLUSTER-SUPPORT.md](docs/CLUSTER-SUPPORT.md)**.


## Getting started

```ts
import { WorkerPool, DistributedLimiter } from '@destroyer777777/betterqueue';

// 1. Start the global worker pool ONCE (e.g. in your app's startup).
const pool = await WorkerPool.create({
  redis: { host: 'localhost', port: 6379 },
  workers: 8,
});

// 2. Create a limiter for each queue (e.g. per tenant).
const limiter = new DistributedLimiter({
  id: 'scope:123456789',
  maxConcurrent: 2,
  minTime: 500,
  reservoir: 10,
  reservoirRefreshInterval: 1_000,
  reservoirRefreshAmount: 5,
});

// 3. Schedule work.
const results = await Promise.all([
  limiter.schedule(async () => 'job-1'),
  limiter.schedule(async () => 'job-2'),
  limiter.schedule(async () => 'job-3'),
]);

console.log(results);            // [ 'job-1', 'job-2', 'job-3' ]
console.log(await limiter.getStats());
// { queueSize: 0, runningCount: 0, reservoir: 4 }

await pool.stop();
```

Running that file against a real Redis produces:

```
results: [ 'job-1', 'job-2', 'job-3' ]
elapsed: 1057 ms
stats: { queueSize: 0, runningCount: 0, reservoir: 4 }
```

The three jobs take about a second because `minTime: 500` spaces job *starts*
by 500 ms, and the reservoir started at 10 tokens and ended at 4 (three
consumed, one more consumed by the refill window).

A runnable version of this example lives in `examples/basic.ts`.

### Gotchas & common mistakes

- **Your function must not resolve before its work is done.** BetterQueue
  measures the job's lifetime by the promise you return.

  ```ts
  // BAD — returns before the work finishes
  limiter.schedule(() => { tasks.forEach((t) => process(t)); });

  // GOOD — waits for everything
  limiter.schedule(() => Promise.all(tasks.map((t) => process(t))));
  ```

- **Bind methods.** If you pass an object's method, bind it.

  ```ts
  limiter.schedule(() => object.doSomething());      // preferred
  limiter.schedule(object.doSomething.bind(object)); // also fine
  ```

- **Always `await pool.stop()` on shutdown.** It closes the Redis connections
  and deregisters the pool from the live-pool registry.

- **`WorkerPool.create()` must be called before you construct a
  `DistributedLimiter`.** Otherwise the constructor throws
  `WorkerPoolNotInitializedError`.

- **`stop()` on a limiter does not wait for jobs.** Call `await
  limiter.drain()` first if you need to wait.

---

## Core concepts

### Queues and limiters

A **queue** is a named bucket of work, identified by a string `id`. A
**limiter** is the JavaScript object that points at a queue and carries the
rate-limit configuration for it.

```ts
const limiter = new DistributedLimiter({
  id: 'tenant:acme:feature:export',
  maxConcurrent: 2,
  minTime: 250,
});
```

The `id` is the only thing that defines the queue. Two `DistributedLimiter`
instances with the same `id` — in the same process or in different processes —
refer to the same logical queue, and they share its concurrency slots, its
`minTime` pacing and its reservoir tokens.

The limiter object itself is deliberately thin. It holds resolved configuration
and a reference to the pool; it holds no timers, no job array, and no queue
state. All of that lives in Redis:

```
DistributedLimiter instance:       WorkerPool (global, one per process):
  id: "tenant:acme"     (string)     pendingJobs Map:
  opts: {...config...}  (object)       "uuid-1" → {fn, resolve, reject}  ← only while pending
  pool: ref             (pointer)      "uuid-2" → ...
  ─────────────────────────────         (deleted on completion)
```

`pendingJobs` is the only mutable in-process state. Its size equals the number
of jobs that have been `schedule()`d but have not yet completed — bounded by
your application's concurrency, not by the number of limiters.

### The reservoir (token bucket)

The reservoir is a token bucket. Each job consumes one token. When the bucket
is empty, jobs wait. There are two ways to refill it, and they mean different
things.

**Refresh — a fixed quota per window.** Every interval the bucket is **reset**
to `reservoirRefreshAmount`.

```ts
new DistributedLimiter({
  id: 'quota:per-minute',
  reservoir: 100,                    // initial token count
  reservoirRefreshAmount: 100,       // tokens the bucket is RESET to
  reservoirRefreshInterval: 60_000,  // every 60 seconds
  maxConcurrent: 1,
  minTime: 333,
});
```

This is "100 requests per minute". Note the bucket *resets*: if you consumed
90 tokens and the window rolls over, you are back to 100, not 110.

**Increase — a quota that trickles back.** Every interval the bucket **gains**
`reservoirIncreaseAmount` tokens, up to an optional cap.

```ts
new DistributedLimiter({
  id: 'quota:trickle',
  reservoir: 40,                     // start with 40
  reservoirIncreaseAmount: 2,        // +2 tokens each interval
  reservoirIncreaseInterval: 1_000,  // every second
  reservoirIncreaseMaximum: 40,      // never exceed 40
  maxConcurrent: 5,
  minTime: 250,
});
```

This is "40 now, +2 every second, capped at 40".

**Refresh and increase are mutually exclusive.** If both are configured, the
refresh (reset) wins — this matches Bottleneck's behaviour, and the Lua
implementation checks refresh first.

```ts
// Reset: the bucket is SET to 10 every second.
new DistributedLimiter({
  id: 'q',
  reservoir: 10,
  reservoirRefreshAmount: 10,
  reservoirRefreshInterval: 1000,
});

// Add: the bucket GROWS 10 → 12 → 14 → … (capped by reservoirIncreaseMaximum).
new DistributedLimiter({
  id: 'q',
  reservoir: 10,
  reservoirIncreaseAmount: 2,
  reservoirIncreaseInterval: 1000,
  reservoirIncreaseMaximum: 40,
});
```

**There is no per-limiter refill timer.** The bucket is maintained *lazily*:
every dequeue attempt and every stats read checks whether an interval has
elapsed and applies the refill inside the same Lua script. That is why the
bucket stays accurate even when the queue is completely idle, and why you do
not need a timer per limiter.

### `maxConcurrent` vs `minTime`

These are orthogonal limits and are usually used together.

- **`maxConcurrent`** caps how many jobs of this queue may be *executing at the
  same time*. It is a hard ceiling: `SCARD` of the running SET is compared
  against it inside the atomic dequeue script.
- **`minTime`** spaces job *starts* by at least N milliseconds, measured from
  the moment the previous job was dequeued (`bq:{id}:lastRun`).

| | `maxConcurrent` | `minTime` |
|---|---|---|
| Limits | simultaneous executions | start-to-start spacing |
| Measured by | `SCARD bq:{id}:running` | `bq:{id}:lastRun` timestamp |
| Applies to | all jobs in the queue | all jobs in the queue |
| Use for | "never more than 2 in flight" | "at most one every 333 ms" |

`maxConcurrent: 1, minTime: 0` gives you a strictly sequential queue.
`maxConcurrent: 10, minTime: 0` gives you pure concurrency with no pacing.
`maxConcurrent: 1, minTime: 500` gives you one job every 500 ms, no overlap.

Note that `minTime` is measured between *dequeues*, not between completions.
A job that takes 10 seconds does not push the next start out by 10 seconds —
only the 10-second duration of the job itself overlaps with the next start
unless `maxConcurrent` forbids it.

### Priority and the job score

Every job carries a priority, an integer where **lower = more important**.
The default is `5`; the convention in this library and in Bottleneck is the
range `1` (highest) to `10` (lowest), and the Lua layer clamps the value to
`0…99` so a composed score can never lose precision.

The score used to order the ready ZSET is:

```
score = priority × 1e12 + arrivalSeq
```

- `priority` occupies the **high-order digits**. A priority-1 job always sorts
  ahead of a priority-9 job, no matter which arrived first.
- `arrivalSeq` is a monotonic counter kept in the queue's `meta` HASH and
  incremented with `HINCRBY`. It breaks ties, so jobs of the same priority
  run **FIFO**.

`1e12` leaves room for 1e12 arrivals within a single priority band, and keeps
every composed score an exact integer (Redis scores are IEEE-754 doubles, so
integers are exact up to 2^53).

```
priority 1, 2nd arrival  → 1 × 1000000000000 + 2 = 1000000000002
priority 9, 3rd arrival  → 9 × 1000000000000 + 3 = 9000000000003
```

**The score is composed server-side.** `enqueue.lua` reads the Redis server
clock with `TIME` and computes the score inside Redis, not in Node.js. Two
consequences:

1. **Priority ordering cannot be broken by client clock skew.** A machine
   with a fast or slow clock cannot jump the queue.
2. **The arrival sequence is global.** Because `HINCRBY meta seq` happens in
   Redis, the tie-break order is the real Redis-side arrival order across all
   processes — not "whichever client called first".

Priority is applied to *ready* jobs. A delayed job keeps its due time as its
score in the `delayed` ZSET; when it is promoted into the ready set it
receives a fresh priority score at that moment.

### The two-ZSET design

Each queue keeps its jobs in **two separate sorted sets**, and the split is
deliberate:

```
bq:{id}:jobs      ZSET   READY   score = priority × 1e12 + arrivalSeq
bq:{id}:delayed   ZSET   WAITING score = due time (unix ms)
```

**Why they are separate.** The two sets are ordered by completely different
quantities. The ready set is ordered by *importance* (priority first, arrival
second). The delayed set is ordered by *time* (earliest due time first). If
they were merged into one ZSET you would have to pick a single score
semantics, and either priority or due time would be lost. Splitting them means
each set can use the ordering it actually needs, and both are O(log n)
Redis-native structures.

The consequence of keeping them apart is that "not yet due" is a *different
state* from "ready but rate-limited", and the library can tell them apart:

- A job in `delayed` is **not eligible**. It becomes eligible when
  `now >= due`.
- A job in `jobs` is **eligible**, but the rate limits (`maxConcurrent`,
  `minTime`, reservoir) may still hold it back.

The promotion from `delayed` → `jobs` happens **inside the dequeue script**,
atomically, so a delayed job can never be observed "half-promoted":

```lua
local dueJobs = redis.call('ZRANGEBYSCORE', delayedKey, '-inf', fmtScore(now))
if #dueJobs > 0 then
  for i = 1, #dueJobs do
    redis.call('ZREM', delayedKey, dueJobs[i])
    local prio = ...
    local seq  = redis.call('HINCRBY', metaKey, 'seq', 1)
    redis.call('ZADD', jobsKey, fmtScore(prio * PRIORITY_SCALE + seq), dueJobs[i])
  end
end
```

Retry backoff uses the same mechanism: a failed job is re-enqueued with a
*relative* delay, so the Redis server clock — not the client clock — decides
when it becomes due.

---

## Multiple processes and distributed usage

Each process creates its own `WorkerPool`. Two processes pointing at the same
Redis are two independent engines that share one set of queues.

### The pool registry (`bq:pools`)

Every pool announces itself in a ZSET whose score is its last heartbeat:

```
bq:pools   ZSET   member = poolId, score = last heartbeat (ms, server clock)
```

The heartbeat is refreshed by the same global ticker that drains the wakeup
ZSET, so it costs no extra timer. A pool that stops heartbeating falls out of
the live window automatically (`POOL_LIVE_WINDOW_MS` = 30 s), which means the
registry **self-cleans** — no garbage collector, no lease cleanup job.

### Per-pool notification channels

This is the part that makes multi-process operation safe, and it is worth
understanding precisely.

A job's executable function is a **JavaScript closure**. It exists only in
the process that created it. Redis can store data; it cannot store a closure.
Therefore:

> **Only the process that scheduled a job can ever run it.**

Every `WorkerPool` consequently listens on its **own** notification channel:

```
bq:workers:notify:<poolId>   LIST   the queue ids this pool should look at
```

Each worker does `BRPOP bq:workers:notify:<its own poolId>`. If every pool
shared one channel, process B could steal process A's notification, and A's
job would sit in Redis forever with nobody looking at it. Per-pool channels
make that impossible: a notification can only ever be consumed by the process
it was addressed to.

### Owner-stamped jobs

Every job payload carries the id of the pool that scheduled it:

```json
{
  "id": "0b71fa75-ca3e-4618-9bcd-29fa3826e0eb",
  "priority": 5,
  "maxRetries": 3,
  "retryCount": 0,
  "enqueuedAt": 1793311108964,
  "owner": "996091-f15cee82"
}
```

`owner` is `<pid>-<8 hex chars>`, unique per `WorkerPool` instance.

The dequeue script uses it to find jobs this pool can actually run. It scans
the ready ZSET for the first entry whose `owner` matches the calling pool
(payloads with no `owner` field are treated as claimable by anyone, for
backwards compatibility):

```lua
local function firstOwned(candidates)
  for i = 1, #candidates do
    local p = candidates[i]
    local o = ownerOf(p)
    if (not o) or (o == ownerId) then
      return p
    end
  end
  return nil
end
```

### How two processes coordinate on one queue id

Given two processes, `A` and `B`, both using queue id `shared`:

1. `A` calls `schedule()`. The closure is stored in `A`'s `pendingJobs` map.
   `enqueue.lua` writes the job into `bq:shared:jobs` with `owner = A`, and
   `LPUSH`es `shared` onto `bq:workers:notify:<A>`.
2. `B` calls `schedule()`. Same thing, but `owner = B` and the notification
   goes to `bq:workers:notify:<B>`.
3. `enqueue.lua` also *fans out* to every other live pool in `bq:pools`, so
   both processes learn that the shared queue has work.
4. When `A`'s worker pops its notification and runs `dequeue.lua`, it scans
   the ready ZSET and picks the first entry owned by `A` — skipping `B`'s
   jobs. The scan is bounded by `DEQUEUE_SCAN_WINDOW` (256 entries) so it
   cannot become unbounded.
5. Rate-limit accounting — `maxConcurrent`, `minTime`, the reservoir — is
   **global**. The running SET, the `lastRun` timestamp and the token count
   are shared Redis structures, so `maxConcurrent: 1` really does mean "one
   job of this queue at a time across the whole cluster".

So the two processes produce **one shared queue with one global ordering**,
and each process runs only the jobs whose closures it holds.

> **Read this before deploying multiple processes.** The coordination above
> works, and a job whose concurrency slot is held by *another* process is
> re-notified as soon as that slot is released. The details are in
> [Known issues and limitations](#known-issues-and-limitations).

---

## Redis schema

Every key is namespaced under `bq:`. All per-queue keys are derived from the
limiter's `id`:

```
bq:{id}:jobs          bq:{id}:delayed      bq:{id}:running
bq:{id}:inflight      bq:{id}:meta         bq:{id}:reservoir
bq:{id}:lastRun       bq:{id}:lastRefresh  bq:{id}:lastIncrease
bq:workers:notify:{poolId}   bq:global:wakeup    bq:pools
```

| Key | Type | Purpose | TTL |
|---|---|---|---|
| `bq:{id}:jobs` | ZSET | **Ready** jobs. Score = `priority × 1e12 + arrivalSeq`. | limiter `ttl` |
| `bq:{id}:delayed` | ZSET | Jobs **not yet due** (`scheduleAt`, retry backoff). Score = due time (unix ms). | limiter `ttl` |
| `bq:{id}:running` | SET | Job IDs currently executing. Enforces `maxConcurrent`. | limiter `ttl` |
| `bq:{id}:inflight` | HASH | `jobId → payload`, for crash recovery. | limiter `ttl` |
| `bq:{id}:meta` | HASH | Queue config (`maxConcurrent`, `minTime`, reservoir settings, `ttl`) **plus the `seq` arrival counter**. | limiter `ttl` |
| `bq:{id}:reservoir` | STRING (int) | Current token count. | limiter `ttl` |
| `bq:{id}:lastRun` | STRING (ms) | Timestamp of the last dequeue. Enforces `minTime`. | limiter `ttl` |
| `bq:{id}:lastRefresh` | STRING (ms) | Timestamp of the last reservoir **refresh**. | limiter `ttl` |
| `bq:{id}:lastIncrease` | STRING (ms) | Timestamp of the last reservoir **increase**. | limiter `ttl` |
| `bq:workers:notify:{poolId}` | LIST | Per-pool worker signal channel. Carries queue ids to look at. | none (deleted on `pool.stop()`) |
| `bq:global:wakeup` | ZSET | Scheduled wakeups. Member = `<poolId>\|<queueId>`, score = wakeup time (ms). | none |
| `bq:pools` | ZSET | Live pool registry. Member = `poolId`, score = last heartbeat (ms). | none (self-cleaning) |

### `meta` — the queue configuration hash

```
maxConcurrent             reservoirRefreshAmount
minTime                   reservoirIncreaseInterval
reservoir                 reservoirIncreaseAmount
reservoirRefreshInterval  reservoirIncreaseMaximum
ttl
seq                       ← monotonic arrival counter (HINCRBY)
```

`seq` lives in the same hash as the config because the ZSET score needs both
the priority and a tie-break sequence, and the hash is read in the same
script.

### Idle cleanup

When a queue drains completely (`pending == 0` and `running == 0`),
`complete.lua` **immediately deletes** the volatile runtime keys:

```
DEL bq:{id}:running  bq:{id}:inflight  bq:{id}:jobs
DEL bq:{id}:delayed  bq:{id}:lastRun
ZREM bq:global:wakeup <poolId>|<queueId>
```

The `meta` key survives so that recreating the limiter is cheap, and any
pending wakeup entry is removed so the global ticker does not keep waking a
queue that no longer exists.

Observed directly:

```
keys after queue went idle:
   bq:pools            type=zset
   bq:probe:clean:meta type=hash
```

### A note on TTLs

The limiter's `ttl` option (default **3600 s**) is what the Lua scripts apply
to the per-queue keys. However, `WorkerPool` also has an internal
`defaultTtl` of **86400 s** that it passes to the scripts for the job-lifecycle
keys. The practical effect, observed on a live instance:

| Key | TTL while a job is queued | TTL after the queue drains |
|---|---|---|
| `bq:{id}:meta` | limiter `ttl` | `86400` (pool default) |
| `bq:{id}:jobs` / `running` / `inflight` / `lastRun` | limiter `ttl` | deleted (idle cleanup) |
| `bq:{id}:reservoir` / `lastRefresh` / `lastIncrease` | limiter `ttl` | limiter `ttl` |

In other words: while the queue is busy, your `ttl` governs. Once it is idle,
the config hash is kept alive for a day so a re-created limiter does not have
to re-sync. If you need the keys to disappear sooner, set a smaller `ttl` and
be aware that the `meta` hash will still be refreshed to the pool default on
the completion path.

---

## API reference

### `WorkerPool.create(options)`

Initialise the global worker pool. Call once at startup, before creating any
limiter.

```ts
const pool = await WorkerPool.create({
  redis: { host: 'localhost', port: 6379 },  // ioredis options
  workers: 8,                 // concurrent workers (default: 64)
  tickIntervalMs: 50,         // global ticker interval (default: 50)
  visibilityTimeoutMs: 300_000, // crash recovery timeout (default: 5 min)
  pendingJobTimeoutMs: 300_000, // orphan sweeper timeout (default: 5 min, 0 = off)
});
```

The `redis` object is passed straight to `ioredis`, so any
[`ioredis` option](https://github.com/luin/ioredis/blob/main/API.md) works —
`host`, `port`, `username`, `password`, `tls`, `db`, etc.

`WorkerPool.create()` is **idempotent**: calling it again returns the
existing pool and ignores the new options.

`WorkerPool` also exposes:

```ts
WorkerPool.getInstance();          // the active pool; throws if not created
await pool.stop();                  // graceful shutdown
pool.poolId;                        // this pool's unique id
pool.commandClient;                 // the shared ioredis client
pool.pendingJobs;                   // Map<string, PendingJob>
```

### `new DistributedLimiter(options)`

Create a rate-limited queue. Instantiation is cheap — the instance holds
configuration only; the queue itself lives in Redis.

```ts
const limiter = new DistributedLimiter({
  id: 'scope:123456789',           // required — unique queue key
  maxConcurrent: 2,                // max parallel jobs (default: 1)
  minTime: 500,                    // ms between job starts (default: 0)
  reservoir: 10,                   // initial token count (default: unlimited)
  reservoirRefreshInterval: 1_000, // token refill interval ms (default: 0)
  reservoirRefreshAmount: 5,       // tokens RESET to on each refill (default: 0)
  reservoirIncreaseInterval: 0,    // token INCREASE interval ms (default: 0)
  reservoirIncreaseAmount: 0,      // tokens ADDED on each increase (default: 0)
  reservoirIncreaseMaximum: 0,    // cap for increase mode (default: 0 = unbounded)
  priority: 5,                     // default priority 1–10 (default: 5)
  ttl: 3_600,                      // Redis key TTL seconds (default: 3600)
  retryOnFailure: true,            // retry failed jobs (default: true)
  maxRetries: 3,                   // max retry attempts (default: 3)
});
```

The limiter is an `EventEmitter` and also exposes `limiter.id`.

### `limiter.schedule(fn, options?)`

Enqueue an async function. Returns a Promise that resolves with `fn`'s return
value.

```ts
const data = await limiter.schedule(
  () => fetch('/api/endpoint').then((r) => r.json()),
  {
    priority: 1,                    // override priority (1 = highest, 10 = lowest)
    scheduleAt: Date.now() + 5_000, // delay job by 5 seconds
    maxRetries: 5,                  // override max retries for this job
  },
);
```

Bottleneck's argument order (`schedule(options, fn)`) is also supported, so
either style compiles:

```ts
await limiter.schedule({ priority: 1 }, () => doWork());
```

`schedule()` throws `LimiterStoppedError` if the limiter has been stopped, and
`TypeError` if you pass options without a function.

### Observability

```ts
// All stats come from Redis — never from memory.
await limiter.getStats();        // { queueSize, runningCount, reservoir }
await limiter.getQueueSize();    // number of pending jobs (ready + delayed)
await limiter.getRunningCount(); // number of executing jobs
await limiter.getReservoir();    // current token count (null if not configured)
```

`getStats()` performs the same lazy reservoir maintenance as the dequeue
script, so `reservoir` is accurate even when the queue is completely idle —
no timer needed to keep it honest.

`queueSize` counts **ready + delayed** jobs: everything still pending.

### Lifecycle

```ts
await limiter.updateSettings({ minTime: 100, reservoir: 20 }); // hot-reload config
await limiter.drain();  // wait for all queued + running jobs to finish
limiter.stop();         // no longer accept new jobs (non-blocking)
await pool.stop();      // gracefully stop all workers
```

`drain()` polls Redis every 100 ms and resolves when `queueSize === 0` and
`runningCount === 0`. It then emits `drained`. Pass a timeout to bound it:

```ts
await limiter.drain(30_000); // rejects after 30s if the queue will not empty
```

`stop()` on a limiter marks it stopped (so `schedule()` throws
`LimiterStoppedError`), detaches it from the pool's shared emitter, and
removes all of its event listeners.

`pool.stop()` stops the ticker and the sweeper, rejects any still-pending
promises with `OrphanedJobError`, stops all workers, deregisters the pool from
`bq:pools`, deletes its notification channel, and closes the Redis
connection.

### Events

```ts
limiter.on('executing', (jobId) => console.log('started:', jobId));
limiter.on('done',      (jobId) => console.log('done:', jobId));
limiter.on('failed',    (jobId, err) => console.error('failed:', jobId, err));
limiter.on('retry',     (jobId, attempt) => console.log('retry:', jobId, attempt));
limiter.on('drained',   () => console.log('queue empty'));
```

Events are scoped to the limiter's own queue id — a limiter never sees
another queue's jobs. The listener signatures are typed, so TypeScript will
catch a mismatch.

| Event | Payload | Fired when |
|---|---|---|
| `executing` | `(jobId: string)` | The job's function has been invoked. |
| `done` | `(jobId: string)` | The job resolved successfully. |
| `failed` | `(jobId: string, error: unknown)` | The job failed and the retry budget was exhausted (or re-enqueueing failed). |
| `retry` | `(jobId: string, attempt: number)` | The job threw and is being retried. `attempt` is 1-based. |
| `drained` | `()` | `drain()` observed an empty queue. |

A complete example:

```ts
import { WorkerPool, DistributedLimiter } from '@destroyer777777/betterqueue';

const pool = await WorkerPool.create({
  redis: { host: 'localhost', port: 6379 },
});

const limiter = new DistributedLimiter({ id: 'demo', maxConcurrent: 1, maxRetries: 1 });

limiter.on('executing', (jobId) => console.log('  ▶ start', jobId.slice(0, 8)));
limiter.on('done',      (jobId) => console.log('  ✓ done ', jobId.slice(0, 8)));
limiter.on('retry',     (jobId, attempt) => console.log('  ↻ retry', jobId.slice(0, 8), attempt));
limiter.on('failed',    (jobId, err) => console.error('  ✗ fail ', jobId.slice(0, 8), err));
limiter.on('drained',   () => console.log('  ∅ drained'));

await limiter.schedule(async () => 'ok');
await limiter.schedule(async () => { throw new Error('boom'); }).catch(() => {});

await limiter.drain(10_000);
await pool.stop();
```

Observed output:

```
  ▶ start ba4b9b7b
  ✓ done  ba4b9b7b
  ▶ start 573b8ff9
  ↻ retry 573b8ff9 1
  ▶ start 573b8ff9
  ✗ fail  573b8ff9 JobMaxRetriesError: Job 573b8ff9-… failed after 2 attempt(s): boom
  ∅ drained
```

Note that `drained` is emitted by `drain()` itself — it is not a background
notification that fires the moment a queue becomes empty.

### Retries

`maxRetries` counts **retries**, not total attempts. With `maxRetries: 1` the
job runs at most twice: the first attempt plus one retry. The `retry` event
carries the 1-based attempt number, and the final failure reports the total
number of attempts:

```
maxRetries: 1  →  2 attempts total  →  "failed after 2 attempt(s)"
maxRetries: 3  →  4 attempts total  →  "failed after 4 attempt(s)"
```

Retries use **exponential backoff** of `1s, 2s, 4s, 8s, …`, capped at 30 s.
The delay is applied as a *relative* delay, so the Redis server clock — not
the client clock — decides when the retry becomes due.

If `retryOnFailure` is `false`, the job is not retried at all and the promise
rejects on the first failure.

### Errors

All library errors extend `BetterQueueError`.

| Error | Thrown when |
|---|---|
| `BetterQueueError` | Base class for everything below. |
| `WorkerPoolNotInitializedError` | A `DistributedLimiter` was constructed before `WorkerPool.create()`. |
| `LimiterStoppedError` | `schedule()` was called on a stopped limiter. |
| `JobMaxRetriesError` | A job failed and its retry budget was exhausted. Carries `jobId`, `attempts` and `cause`. |
| `OrphanedJobError` | A pending job was evicted without executing (lost notification, or the pool was stopped). |
| `RedisCommandError` | A Redis command failed unexpectedly. |

```ts
import { JobMaxRetriesError } from '@destroyer777777/betterqueue';

try {
  await limiter.schedule(async () => { throw new Error('upstream 500'); });
} catch (err) {
  if (err instanceof JobMaxRetriesError) {
    console.error(err.jobId, err.attempts, err.cause);
  }
}
```

`JobMaxRetriesError` includes the underlying error's message, so
`err.message` reads `Job <id> failed after 2 attempt(s): upstream 500`.

---

## Full options reference

Every option for `DistributedLimiter`, `WorkerPool` and `LimiterManager` is
listed in **[docs/OPTIONS.md](docs/OPTIONS.md)**.
## Crash recovery

If your process crashes while jobs are executing:

1. On restart, `recoverQueue()` runs automatically on each limiter's first
   use (the limiter lazily initialises on its first `schedule()` or stats
   read).
2. It scans the `bq:{id}:inflight` HASH for jobs older than
   `visibilityTimeoutMs` (5 minutes by default).
3. Eligible jobs are re-enqueued in the ready ZSET with an incremented retry
   counter, keeping their priority. If the retry budget is exhausted, the
   job is dropped.
4. The owning pool is notified so the queue is looked at again.

> **The honest limitation.** The job's closure lived in the crashed process,
> so a recovered job **cannot be executed**. Recovery preserves the queue's
> ordering, its counters and its token accounting, and it makes the
> backlog visible again — but the work itself must be **re-submitted by your
> application**. The original `Promise` was lost with the process and can
> never be settled.

In other words, crash recovery is *bookkeeping* recovery, not *execution*
recovery. If your jobs must survive a crash and still run, you need a
durable job store (a database, SQS, and so on) that holds the work itself and
re-submits it — BetterQueue holds the closure, and closures do not survive a
crash.

The visibility timeout is the trade-off knob. A shorter
`visibilityTimeoutMs` recovers stale slots sooner, at the cost of possibly
re-queuing a job that was merely slow. A longer one is safer for long jobs
but leaves the queue blocked for longer after a crash. Set it to at least your
longest expected job duration.

---

## Bottleneck compatibility layer

`src/LimiterManager.ts` provides a drop-in-shaped manager for code that was
written against a Bottleneck-based `LimiterManager`. It is exported both from
the package root and as its own subpath:

```ts
import limiterManager from 'better-queue/LimiterManager';
// or
import { limiterManager } from '@destroyer777777/betterqueue';
```

Both `better-queue` and `better-queue/LimiterManager` are valid import paths.

### Side-by-side

**Before — Bottleneck:**

```ts
import Bottleneck from 'bottleneck';

const limiter = new Bottleneck({
  id: 'member-add',
  maxConcurrent: 1,
  reservoir: 10,
  reservoirRefreshAmount: 10,
  reservoirRefreshInterval: 10_000,
  minTime: 1_000,
  timeout: 3_600_000,
});

const result = await limiter.schedule({ id: 'job-123' }, async () => {
  await doWork();
});
```

**After — BetterQueue:**

```ts
import { WorkerPool } from '@destroyer777777/betterqueue';
import limiterManager from 'better-queue/LimiterManager';

await WorkerPool.create({ redis: { host: 'localhost', port: 6379 } });

limiterManager.registerFeatureConfig('member-add', {
  maxConcurrent: 1,
  reservoir: 10,
  reservoirRefreshAmount: 10,
  reservoirRefreshInterval: 10_000,
  minTime: 1_000,
  timeout: 3_600_000,
});

const limiter = limiterManager.getLimiter(scopeId, 'member-add');
const result = await limiter.schedule({ id: 'job-123' }, async () => {
  await doWork();
});
```

The `LimiterWrapper.schedule()` accepts both `schedule(fn)` and
`schedule(options, fn)`, exactly like Bottleneck.

### Using `DistributedLimiter` directly

If you are migrating code that uses `Bottleneck` instances rather than a
manager, the migration is equally small:

```ts
// Before
import Bottleneck from 'bottleneck';
const limiter = new Bottleneck({ maxConcurrent: 2, minTime: 250 });
await limiter.schedule(() => doWork());

// After
import { WorkerPool, DistributedLimiter } from '@destroyer777777/betterqueue';
await WorkerPool.create({ redis: { host: 'localhost' } });
const limiter = new DistributedLimiter({ id: 'my-queue', maxConcurrent: 2, minTime: 250 });
await limiter.schedule(() => doWork());
```

The one thing you must add is the `id`, because in BetterQueue the `id` *is*
the queue identity — there is no separate "cluster id".

### Behavioural differences — read these

Migrating is a one-import change, but the semantics are not identical. The
differences, honestly:

| Area | Bottleneck | BetterQueue |
|---|---|---|
| **Priority scope** | Local to a single limiter. Across a cluster, another limiter may run a lower-priority job first. | **Global across the whole shared queue** — the ordering is computed in one Redis ZSET. |
| **Arrival order** | Only guaranteed per limiter; across a cluster a later job may run first. | **Global** — a server-side monotonic sequence breaks ties. |
| **Queue durability** | In memory. Exiting the process loses the queue. | **In Redis.** The job *list* survives a restart — but the closure does not, so a recovered job must be re-submitted by your application. |
| **Process restart** | In-flight closures are simply gone. | In-flight closures are gone too. Pending promises are rejected with `OrphanedJobError` when the pool stops. |
| **Delayed jobs** | Not supported. | `scheduleAt` is supported. |
| **Timeouts** | Native per-job `expiration`. | `timeout` in `FeatureConfig` is a client-side `Promise.race` in `LimiterWrapper`. `DistributedLimiter` has no per-job timeout; wrap your own function if you need one. |
| **Weights** | `weight` consumes several reservoir tokens and several concurrency slots. | Not implemented. One job = one token = one slot. |
| **Load shedding** | `highWater` + `strategy` (`LEAK`, `OVERFLOW`, `BLOCK`). | Not implemented. |
| **Batching** | `Bottleneck.Batcher`. | Not implemented. |
| **`Group` / `Cluster`** | `Group` manages many limiters; `Cluster` groups limiters across processes. | Not reimplemented as an API. The same effect is achieved by keying queues in Redis: one queue id per logical group. |
| **`counts()`** | Full `RECEIVED`/`QUEUED`/`RUNNING`/`EXECUTING`/`DONE` counters. | `RECEIVED` and `DONE` are always `0`. `QUEUED`/`RUNNING`/`EXECUTING` are accurate. |
| **`wrap()`, `submit()`** | Supported. | Not implemented. Use `schedule()`. |
| **`chain()`** | Supported. | Not implemented. |
| **Events** | `error`, `failed`, `retry`, `empty`, `idle`, `dropped`, `depleted`, `debug`, lifecycle events. | `executing`, `done`, `failed`, `retry`, `drained`. |

> The Bottleneck `Group`/`Cluster` API is **not** reimplemented — BetterQueue
> achieves the same effect by keying queues in Redis, which is a different
> (and simpler) model.

---

## Lua scripts

All critical operations are implemented as atomic Lua scripts to prevent race
conditions. The scripts live in `scripts/` and are bundled into
`src/lua-scripts.ts` at build time.

| Script | Registered as | Purpose |
|---|---|---|
| `enqueue.lua` | `bqEnqueue` | Add a job to the ready/delayed set, compose the priority score server-side, and signal the owner pool. |
| `dequeue.lua` | `bqDequeue` | Fetch the next eligible job owned by this pool (rate-limit-aware, lazy reservoir refresh/increase, promotes due delayed jobs). |
| `complete.lua` | `bqComplete` | Release the running slot, delete the inflight entry, re-signal, and perform idle cleanup. |
| `wakeup_check.lua` | `bqWakeupCheck` | Global ticker — wake queues whose delay expired. |
| `wakeup_schedule.lua` | `bqWakeupSchedule` | Atomically schedule a worker wakeup, keeping the earliest one. |
| `recover.lua` | `bqRecover` | Re-enqueue orphaned crashed jobs. |
| `stats.lua` | `bqStats` | Atomic stats snapshot (with lazy reservoir maintenance). |
| `pool_heartbeat.lua` | `bqPoolHeartbeat` | Register/refresh this pool's liveness. |
| `pool_unregister.lua` | `bqPoolUnregister` | Deregister a pool and delete its notification channel on shutdown. |

They are registered with `ioredis.defineCommand`, so they are type-safe at
the call site and run atomically inside Redis.

---

## Performance

BetterQueue's design target is not "the highest possible throughput" — it is
**predictable, flat resource usage** as the number of queues and processes
grows. The following numbers were measured on this repository, against an
**isolated Redis on port 6399**, using Node.js v20.20.0 and the built `dist/`
bundle. Benchmarks 1–3 replicate `examples/benchmark.ts` exactly (same job
counts, same limiter settings); Benchmark 4 is an additional idle-footprint
measurement.

```
Benchmark 1 — Raw throughput (1 limiter, maxConcurrent: 100, no rate limits)
  Jobs:        10,000
  Time:        1292ms
  Throughput:  7,740 jobs/sec
  Heap before: 14.99 MB
  Heap after:  84.87 MB
  Heap delta:  69.88 MB

Benchmark 2 — Memory stability (1,000 limiters x 50 jobs)
  Heap after creating 1,000 limiters: 86.53 MB
  Per-limiter cost: 1.69 KB
  Total jobs:      50,000
  Time:            4903ms
  Throughput:      10,198 jobs/sec
  Heap before:     84.88 MB
  Heap after run:  345.63 MB
  Heap after GC:   16.02 MB
  Heap leak:       -68.86 MB

Benchmark 3 — Rate-limited burst (500 limiters x 50 jobs, minTime: 100ms)
  25,000 total
  Time: 8813ms
  Expected min: ~5000ms (minTime=100ms x 50 sequential jobs)
  Heap: 110.31 MB

Benchmark 4 — Idle footprint (10,000 limiters created, no jobs)
  Heap delta:  12.60 MB
  Per-limiter: 1321 bytes
```

**Read these numbers honestly.**

- Throughput here is **~8–10k jobs/sec** for trivial jobs on a single local
  Redis with 30 workers. Every job is a Redis round trip plus a Lua script
  invocation; the Lua scripts are the cost centre. This is the price of
  moving the queue into Redis: a single-process, in-memory limiter will
  generally sustain a higher job rate, because BetterQueue trades local
  throughput for a shared, inspectable, globally-ordered queue.
- The heap **grows while jobs are in flight** (69 MB for 10k pending jobs)
  and **returns to baseline after GC**. The `-68.86 MB` "heap leak" figure in
  Benchmark 2 is the GC reclaiming the transient allocation — the steady-state
  footprint is flat.
- The per-limiter footprint measured is **~1.3–1.7 KB**. That figure includes
  the `EventEmitter` machinery, the per-queue subscriber sets and the
  resolved options object — it is the real cost, not a best case. It is still
  small and — importantly — **flat**: 10 000 idle limiters cost 12.6 MB and,
  crucially, **zero timers**.
- The property that actually matters is the **timer count**, measured
  directly by instrumenting `setInterval`:

  ```
  setInterval calls before pool:                 0
  setInterval calls after WorkerPool.create:      2   (ticker + orphan sweeper)
  setInterval calls after creating 500 limiters:  0
  setInterval calls after running a job:          0
  ```

  Two timers per process, no matter whether you have one limiter or a
  million. Bottleneck uses one `setInterval` per limiter
  (`heartbeatInterval`, 250 ms local / 5000 ms clustered).

Run the benchmark yourself with `npm run example:benchmark`. Note that
`examples/benchmark.ts` points at `localhost:6379` by default — edit the port
to `6399` (or another non-production instance) before running it.

### Complexity

| Operation | Complexity | Notes |
|---|---|---|
| `schedule()` | O(log n) | One Redis script: `HINCRBY` + `ZADD` + `LPUSH`. |
| Dequeue | O(log n + w) | `w` = the bounded owner scan window (256 entries). |
| Promote due delayed jobs | O(k log n) | `k` = number of jobs that just became due. |
| `getStats()` | O(1) | Three cardinality reads plus the lazy reservoir check. |
| Global tick | O(b) | `b` = wakeup batch size (100). |

---

## Known issues and limitations

### Fixed — cross-process jobs could stall when the slot was held by another process

**Status:** fixed. Previously this was an open, reproducible bug: when two
processes shared a queue id and the concurrency slot was occupied by a job
belonging to **another** process, the waiting job sat in Redis indefinitely
until an unrelated notification happened to poke the queue.

**Why it happened.** `dequeue.lua` checked `maxConcurrent` *before* looking at
whether the queue had any work this pool could run, and returned
`concurrent_full` with `nextEligibleMs = '0'` ("no idea when to retry").
`WorkerPool.drainQueue()` did not schedule a wakeup for that status, so the
job was never re-notified.

**How it was fixed.** `dequeue.lua` now scans for a job owned by the calling
pool *first*, and only then applies the concurrency check to that specific
job. When the slot is held by another process it returns
`concurrent_full` with a real retry time (`now + 50` ms) and
`WorkerPool.drainQueue()` schedules a wakeup for it, so the job is retried
until the slot frees up. As a bonus, `complete.lua` now fans a notification
out to the other live pools when a slot is released, so the waiting job
usually starts on the very next tick rather than on the next poll.

`maxConcurrent` is still enforced globally across processes, and the
single-process path is unchanged — when the slot is held by a job of the
*same* pool, that pool is notified directly on completion and does not poll
at all.

A regression test (`test/cross-process.test.ts`) spawns a real second Node
process and asserts the waiting job starts within a bounded time after the
slot is released.

### Other limitations

- **Redis Cluster is not supported.** The Lua scripts operate on multiple keys
  at once, and the key names do not contain a Redis Cluster hash tag, so
  `CROSSSLOT` errors occur on a clustered deployment. This affects
  Redis Cluster and ElastiCache in cluster mode. A single-node Redis works
  fine.

  This was measured against a live 3-node Redis 7.4 cluster rather than
  assumed:

  ```
  current key layout         -> CROSSSLOT Keys in request don't hash to the same slot
  all queue keys, no globals -> OK
  queue + bq:global:wakeup   -> CROSSSLOT
  ```

  The fix is a key-layout change plus a client-side notify fan-out. The full
  analysis, the migration plan and the test plan are in
  **[docs/CLUSTER-SUPPORT.md](docs/CLUSTER-SUPPORT.md)**.

- **Redis Sentinel is supported.** Pass the sentinels and the master name
  instead of a host and port:

  ```ts
  const pool = await WorkerPool.create({
    redis: {
      sentinels: [{ host: '127.0.0.1', port: 26379 }],
      name: 'mymaster',
    },
  });
  ```

  The Sentinel discovers the current primary and `ioredis` follows it through
  a failover. No key layout change, no script change — the queue state and
  the atomicity guarantees are identical to a single-node deployment. The
  failover path is covered by a test that kills the primary outright and
  asserts that jobs keep running once the replica has been promoted.
- **The reservoir interval is not aligned to a wall-clock schedule.** Because
  refills are applied lazily on the next dequeue or stats read, a bucket can
  refill slightly later than the exact interval boundary if the queue is
  completely idle. Bottleneck has the same property in its local datastore.
  A refresh is also computed from the moment the limiter was first used.
- **`reservoirRefreshInterval` is not restricted to multiples of 250 ms.**
  Bottleneck requires this because of its own heartbeat granularity; BetterQueue
  computes refills from the Redis server clock, so any interval works.
- **Job weights are not implemented.** One job consumes one token and one
  concurrency slot.
- **Load shedding strategies (`highWater`, `LEAK`, `OVERFLOW`, `BLOCK`) are
  not implemented.**
- **Batching is not implemented.**
- **`Group`/`Cluster` are not reimplemented as APIs.**
- **There is no per-job timeout on `DistributedLimiter`.** Use
  `LimiterManager`'s `timeout`, or wrap your function.
- **A lost Redis notification strands a job until the orphan sweeper runs.**
  If the `BRPOP` notification is lost (Redis restart, network blip), the job
  stays in Redis while its closure sits in `pendingJobs`. After
  `pendingJobTimeoutMs` (5 minutes by default) the sweeper rejects the
  promise with `OrphanedJobError` and the job is picked up again by crash
  recovery.
- **`pendingJobs` grows with the number of pending jobs.** That is by
  design — the closures have to live somewhere — but a queue that accumulates
  a large backlog will hold a large map.

---

## Troubleshooting

### `WorkerPoolNotInitializedError: WorkerPool has not been started. Call WorkerPool.create() before creating limiters.`

You constructed a `DistributedLimiter` before starting the pool. Call
`await WorkerPool.create({ redis: {...} })` at startup, before creating any
limiter.

### `LimiterStoppedError: Limiter "..." has been stopped and is no longer accepting jobs.`

You called `schedule()` after `limiter.stop()`. `stop()` is permanent for that
instance — create a new limiter if you need to accept work again.

### `JobMaxRetriesError: Job ... failed after N attempt(s): ...`

The job threw and the retry budget was exhausted. The original error is on
`err.cause` and its message is included in `err.message`. Raise `maxRetries`,
set `retryOnFailure: false` to fail fast, or fix the underlying error.

Retries use **exponential backoff**: 1 s, 2 s, 4 s, 8 s, capped at 30 s.

### `OrphanedJobError: Job ... was evicted from the pending-jobs map without executing.`

The Redis notification for this job was lost, or the pool was stopped while
the job was pending. The job may still be in Redis. It is re-queued by crash
recovery once it is older than `visibilityTimeoutMs`.

### `Connection is closed.`

You called `schedule()` after `await pool.stop()`. The pool's Redis
connection is gone. Restart the pool before scheduling more work.

### Jobs never run

Check, in order:

1. **Is the pool started?** `WorkerPool.getInstance()` throws if not.
2. **Are you looking at the same queue id?** `bq:{id}:jobs` is the ready set.
   `redis-cli ZCARD 'bq:your:id:jobs'` should be non-zero while jobs wait.
3. **Is a reservoir empty?** `redis-cli GET 'bq:your:id:reservoir'`. If it is
   `0`, jobs are waiting for a refill — and if no dequeue or stats read
   happens, the refill is only applied lazily. Calling `getStats()` will
   apply it.
4. **Is `maxConcurrent` saturated?** `redis-cli SCARD 'bq:your:id:running'`.
5. **Are the jobs in `delayed`?** `redis-cli ZRANGE 'bq:your:id:delayed' 0 -1
   WITHSCORES`. A job there is not due yet.
6. **Is the wakeup ZSET populated?** `redis-cli ZRANGE 'bq:global:wakeup' 0 -1
   WITHSCORES`. If a queue is rate-limited there should be a member
   `<poolId>|<queueId>`.
7. **Is the pool alive?** `redis-cli ZRANGE 'bq:pools' 0 -1 WITHSCORES`. A
   pool whose heartbeat is more than 30 s old is considered dead and is
   removed from the registry.

### `CROSSSLOT Keys in request don't hash to the same slot`

You are running against **Redis Cluster** (or ElastiCache in cluster mode).
BetterQueue's Lua scripts touch multiple keys at once and the key names carry
no hash tag, so they cannot be routed to one slot. Use a single-node Redis or
a Sentinel-managed primary instead. See
[Other limitations](#other-limitations).

### `NOPERM this user has no permissions to run the 'EVAL' command`

Redis ACLs do not allow your user to run Lua scripts. BetterQueue needs
`EVAL`/`EVALSHA`, plus read/write access to the `bq:*` keyspace. Grant
something like:

```
ACL SETUSER app on >password ~bq:* +@all
```

(or a narrower set of commands: `+eval +evalsha +zadd +zrange +zcard +zrem
+zscore +hgetall +hset +hincrby +lpush +lrange +lrem +brpop +sadd +srem
+scard +get +set +del +expire +exists +incr +decr`).

### `READONLY You can't write against a read only replica`

Your Redis connection points at a replica. BetterQueue needs a writable
primary.

### `MOVED` / `ASK` errors

Redis Cluster redirection. See the `CROSSSLOT` entry above — BetterQueue is
not currently Cluster-compatible.

### Connection refused / `ECONNREFUSED`

Redis is not reachable at the host and port you gave `WorkerPool.create()`.
Check `redis-cli -h <host> -p <port> ping`.

### The queue is empty but jobs are still pending

`queueSize` counts **ready + delayed** jobs. If `getQueueSize()` returns 0
while you expect pending work, check `bq:{id}:delayed` — and remember that
the idle-cleanup path deletes the volatile keys entirely once a queue drains,
so "no keys at all" is the normal state for an idle queue.

### Jobs run but the ordering looks wrong

- Confirm `maxConcurrent` is what you think. With a large `maxConcurrent`,
  jobs start immediately and priority has nothing to reorder.
- Priority is clamped server-side to `0…99`. A priority of `0` is the
  highest.
- Within one priority band, ordering is FIFO by the server-side arrival
  sequence.

### Diagnosing with raw Redis

```bash
redis-cli ZRANGE 'bq:my-queue:jobs' 0 -1 WITHSCORES
redis-cli ZRANGE 'bq:my-queue:delayed' 0 -1 WITHSCORES
redis-cli SMEMBERS 'bq:my-queue:running'
redis-cli HGETALL 'bq:my-queue:meta'
redis-cli GET 'bq:my-queue:reservoir'
redis-cli ZRANGE 'bq:global:wakeup' 0 -1 WITHSCORES
redis-cli ZRANGE 'bq:pools' 0 -1 WITHSCORES
redis-cli KEYS 'bq:workers:notify:*'
```

---

## Requirements

- **Node.js** ≥ 18
- **Redis** ≥ 6 (single node or Sentinel-managed primary — **not** Redis Cluster)
- **TypeScript** ≥ 5 if you use the bundled type definitions

Runtime dependencies: `ioredis` and `lru-cache`. Nothing else.

---

## Testing

```bash
npm test
```

The suite runs against a **real Redis instance** — an isolated one on port
`6399` by default (see `test/helpers.ts`). Port `6379` is production Redis
and is never touched by the test suite.

```bash
npm test            # run once
npm run test:watch  # watch mode
npm run typecheck   # tsc --noEmit
npm run build       # bundle lua scripts + build dist/
```

The 34 tests cover basic scheduling, `maxConcurrent`, `minTime`, reservoir
refresh, reservoir increase (including `reservoirIncreaseMaximum`),
`scheduleAt`, priority ordering and the stored score, error + retry
behaviour, stats and `drain()`.

`test/cross-process.test.ts` spawns **real second Node processes** against
the same isolated Redis and asserts that a job waiting on a concurrency slot
held by another process starts as soon as the slot is released, and that the
wakeup ZSET stays bounded.

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full guide. The short version:

1. **Open an issue first** for anything larger than a bug fix, so the design
   can be discussed before the code is written.
2. **Keep the Lua scripts and their TypeScript wrapper in sync.** The canonical
   sources live in `scripts/*.lua`; `src/lua-scripts.ts` is generated from
   them. Run `npm run build:lua` and commit the regenerated file alongside the
   `.lua` change. `npm run build` does both.
3. **Add a test.** The suite runs against a real Redis instance — start one on
   port `6399` (see `test/helpers.ts`) and run `npm test`. Tests must not
   touch port `6379`.
4. **Keep the public API typed.** `DistributedLimiter` and `WorkerPool` carry
   explicit `EventEmitter` overloads so `limiter.on(...)` is type-safe; add
   overloads rather than loosening types.
5. **Do not change the Redis key scheme casually.** Live deployments have
   queues in Redis; a key rename is a breaking change and needs a migration
   path.
6. **Update `README.md` and `HOW_IT_WORKS.md` together.** The options tables
   in the README are the reference for the API surface — if you add an option,
   it needs a row, a type, a default and a description.

Run the full check before opening a pull request:

```bash
npm run typecheck
npm test
npm run build
```

There are a few things we know are missing — a `node-redis` adapter, Redis
Cluster support, a `reset()` method, sliding-window limiting, and job
inspection — and those are listed in `CONTRIBUTING.md` as good first
contributions.

Security issues: see [SECURITY.md](SECURITY.md).

## License

MIT © Saiop68
