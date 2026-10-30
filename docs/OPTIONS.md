# Full options reference

Every option accepted by `DistributedLimiter`, `WorkerPool` and the
`LimiterManager` compatibility layer.

### `WorkerPool.create()` options

| Option | Type | Default | Description |
|---|---|---|---|
| `redis` | `RedisOptions` | — (**required**) | ioredis connection options, passed straight through. |
| `workers` | `number` | `64` | Number of concurrent workers. Each worker holds one blocking (BRPOP) Redis connection. |
| `tickIntervalMs` | `number` | `50` | Interval of the single global ticker that drains `bq:global:wakeup` and refreshes the pool heartbeat. |
| `visibilityTimeoutMs` | `number` | `300000` (5 min) | An inflight job older than this is considered orphaned and re-queued by crash recovery. |
| `pendingJobTimeoutMs` | `number` | `300000` (5 min) | How long a `pendingJobs` entry may live before the orphan sweeper rejects it. `0` disables the sweeper. |

Sizing guidance for `workers`, from the source's own notes: each worker is
one blocking Redis connection (~2 MB RAM), so
`workers ≈ peak_concurrent_queues × jobs_per_queue_per_second × avg_job_duration_seconds`.
64 workers handles roughly 640 concurrent upstream calls; 128 handles ~1280.

### `DistributedLimiter` options

| Option | Type | Default | Description |
|---|---|---|---|
| `id` | `string` | — (**required**) | Unique queue identifier. Becomes the Redis namespace `bq:{id}:*`. |
| `maxConcurrent` | `number` | `1` | Maximum jobs executing simultaneously in this queue, **globally across all processes**. |
| `minTime` | `number` | `0` | Minimum milliseconds between consecutive job **starts**. |
| `reservoir` | `number` | `-1` (unlimited) | Initial token count. Omit or use `-1` to disable the token bucket. `0` is valid (start empty). |
| `reservoirRefreshInterval` | `number` | `0` (disabled) | Interval in ms at which the reservoir is refilled. Requires `reservoir` and `reservoirRefreshAmount`. |
| `reservoirRefreshAmount` | `number` | `0` (disabled) | The value the reservoir is **reset** to on each refresh interval. |
| `reservoirIncreaseInterval` | `number` | `0` (disabled) | Interval in ms at which the reservoir is **incremented**. Requires `reservoir`. |
| `reservoirIncreaseAmount` | `number` | `0` (disabled) | Number of tokens **added** on each increase interval. |
| `reservoirIncreaseMaximum` | `number` | `0` (unbounded) | Maximum value the reservoir may reach in increase mode. |
| `priority` | `number` | `5` | Default job priority. Lower = more important. Conventionally `1`–`10`. |
| `ttl` | `number` | `3600` (1 h) | Redis key TTL in seconds for this queue's keys. |
| `retryOnFailure` | `boolean` | `true` | Whether failed jobs are retried. |
| `maxRetries` | `number` | `3` | Maximum **retries** per job (so `3` means up to 4 attempts in total). |

**Refresh vs increase, one more time.** `reservoirRefreshAmount` **resets**
the bucket to a fixed value; `reservoirIncreaseAmount` **adds** to it. If both
are configured, refresh wins.

### `schedule()` options

| Option | Type | Default | Description |
|---|---|---|---|
| `priority` | `number` | limiter's `priority` | Job priority. Lower = more important. Clamped to `0…99` server-side. |
| `scheduleAt` | `number` | `0` (now) | Unix timestamp in ms. The job will not start before this time. |
| `maxRetries` | `number` | limiter's `maxRetries` (or `0` when `retryOnFailure` is `false`) | Retry budget for this specific job. Counts **retries**, not total attempts. |

### `updateSettings()` patchable fields

`updateSettings()` writes to the queue's `meta` hash in Redis, so the change
takes effect on the next dequeue attempt by **every** process working on that
queue.

```ts
await limiter.updateSettings({
  maxConcurrent: 4,
  minTime: 250,
  reservoir: 20,
  reservoirRefreshInterval: 1_000,
  reservoirRefreshAmount: 10,
  reservoirIncreaseInterval: 0,
  reservoirIncreaseAmount: 0,
  reservoirIncreaseMaximum: 40,
  ttl: 600,
});
```

| Field | Writable | Notes |
|---|---|---|
| `maxConcurrent` | ✅ | |
| `minTime` | ✅ | |
| `reservoir` | ✅ | |
| `reservoirRefreshInterval` | ✅ | |
| `reservoirRefreshAmount` | ✅ | |
| `reservoirIncreaseInterval` | ✅ | |
| `reservoirIncreaseAmount` | ✅ | |
| `reservoirIncreaseMaximum` | ✅ | |
| `ttl` | ✅ | |
| `priority` | ❌ | Not written. The default priority is a client-side setting. |
| `retryOnFailure` | ❌ | Not written. Retry behaviour is decided at schedule time. |
| `maxRetries` | ❌ | Not written. Use the per-job `maxRetries` in `schedule()`. |
| `id` | ❌ | The queue identity is immutable. |

Calling `updateSettings({})` is a no-op and performs no Redis write.

### `LimiterManager` / `FeatureConfig` options

`LimiterManager` is the Bottleneck-compatible layer. `FeatureConfig` is the
per-feature configuration you register at startup.

| Option | Type | Default | Description |
|---|---|---|---|
| `maxConcurrent` | `number` | `10` | Max concurrent jobs per queue. |
| `reservoir` | `number` | `-1` (unlimited) | Token bucket size. Values `<= 0` are treated as unlimited. |
| `reservoirRefreshAmount` | `number` | `10` | Tokens the bucket is reset to on each refresh. |
| `reservoirRefreshInterval` | `number` | `1000` | Refresh interval in ms. |
| `reservoirIncreaseInterval` | `number` | `0` (disabled) | Increase interval in ms. |
| `reservoirIncreaseAmount` | `number` | `0` (disabled) | Tokens added per increase interval. |
| `reservoirIncreaseMaximum` | `number` | `0` (unbounded) | Cap for increase mode. |
| `minTime` | `number` | `0` | Minimum ms between job starts. |
| `timeout` | `number` | `30000` | Per-job timeout in ms. Implemented client-side with `Promise.race`. |
| `ttl` | `number` | `600` (10 min) | Redis key TTL in seconds. |
| `maxRetries` | `number` | `3` | Max **retries** per job. `0` disables retrying (the job fails on its first attempt). |
| `trackDoneStatus` | `boolean` | — | **Ignored.** Accepted for Bottleneck compatibility. BetterQueue always tracks status in Redis, so there is no overhead to disable. |

`registerFeatureConfig(feature, config)` merges your config over the defaults
and stores it in an LRU (max 200 entries). `getLimiter(scopeId, feature)`
creates the limiter lazily and caches it under the key
`` `${scopeId}::${feature}` `` in an LRU (max 10 000 entries, 10-minute TTL).

> **Important.** `timeout` is enforced client-side by racing your function
> against a timer. If your job can legitimately run longer than `timeout`,
> either raise it or make sure the pool's `visibilityTimeoutMs` is **at least**
> as large as the longest job timeout — otherwise crash recovery can re-enqueue
> a job that is still running.

### `LimiterWrapper` / `ScheduleJobOptions`

`LimiterManager.getLimiter()` returns a `LimiterWrapper`, which exposes a
Bottleneck-shaped API:

```ts
const limiter = limiterManager.getLimiter(scopeId, 'member-add');

await limiter.schedule(() => doWork());
await limiter.schedule({ id: 'some-id', priority: 2 }, () => doWork());
```

| `ScheduleJobOptions` field | Type | Description |
|---|---|---|
| `id` | `string` | Optional job identifier, for logging and deduplication. |
| `priority` | `number` | Lower = higher priority. |
| `scheduleAt` | `number` | Unix ms; the job will not start before this time. |

`LimiterWrapper` also provides:

| Method | Returns | Notes |
|---|---|---|
| `schedule(fn)` / `schedule(options, fn)` | `Promise<T>` | Wraps the task in a timeout race. |
| `counts()` | `Promise<{RECEIVED, QUEUED, RUNNING, EXECUTING, DONE}>` | Bottleneck-shaped counters. `RECEIVED` and `DONE` are always `0` — BetterQueue has no in-process queue and Redis cleans up completed jobs. |
| `drain()` | `Promise<void>` | Waits for the queue to empty. |
| `stop()` | `void` | Stops accepting new jobs. |
| `inner` | `DistributedLimiter` | Direct access to the underlying limiter. |

`LimiterManager` itself exposes `registerFeatureConfig`, `getLimiter`,
`removeLimiter`, `removeScopeLimiters`, `clearAll`, `getStats`,
`getCacheUsage` and `drainScope`. The older `removeGuildLimiters` and
`drainGuild` names still work as deprecated aliases.

---

---

See the [main README](../README.md) for the overview, benchmarks and
quickstart.
