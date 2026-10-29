/**
 * Redis key schema for BetterQueue.
 * All keys are namespaced under `bq:` to avoid collisions.
 */
export const KEYS = {
  /** ZSET — ready jobs, score = priority * 1e12 + arrival seq */
  jobs: (id: string) => `bq:${id}:jobs`,
  /** ZSET — jobs not yet due (scheduleAt / retry backoff), score = due time (ms) */
  delayed: (id: string) => `bq:${id}:delayed`,
  /** SET — job IDs of currently executing jobs */
  running: (id: string) => `bq:${id}:running`,
  /** HASH — jobId → full payload, for crash recovery */
  inflight: (id: string) => `bq:${id}:inflight`,
  /** HASH — queue configuration (maxConcurrent, minTime, etc.) + arrival counter */
  meta: (id: string) => `bq:${id}:meta`,
  /** STRING (int) — current token count for reservoir */
  reservoir: (id: string) => `bq:${id}:reservoir`,
  /** STRING (ms) — timestamp of last job dequeue */
  lastRun: (id: string) => `bq:${id}:lastRun`,
  /** STRING (ms) — timestamp of last reservoir refresh */
  lastRefresh: (id: string) => `bq:${id}:lastRefresh`,
  /** STRING (ms) — timestamp of last reservoir increase */
  lastIncrease: (id: string) => `bq:${id}:lastIncrease`,
  /**
   * LIST — per-pool notification channel.
   *
   * Each WorkerPool listens on its OWN channel so that a notification for
   * pool A is never consumed by pool B. Without this, a second process would
   * steal the first process's notification and its job would never run.
   */
  notifyPool: (poolId: string) => `bq:workers:notify:${poolId}`,
  /** ZSET — scheduled worker wakeups, score = wakeup time (ms) */
  wakeup: 'bq:global:wakeup',
  /** ZSET — live WorkerPool registry, score = last heartbeat (ms) */
  pools: 'bq:pools',
} as const;

/**
 * How long (ms) a WorkerPool stays "live" without a heartbeat.
 * A pool that stops heartbeating is considered dead.
 */
export const POOL_LIVE_WINDOW_MS = 30_000;

/** Default TTL for all Redis keys (1 hour). Inactive queues auto-expire. */
export const DEFAULT_TTL_SECONDS = 3_600;

/**
 * Default number of workers in the pool.
 *
 * Each worker = 1 ioredis BRPOP connection (~2MB RAM).
 * Rule of thumb: workers = expected_peak_concurrent_guilds × avg_jobs_per_guild_per_second × avg_job_duration_sec
 *
 * For a bot with 10k guilds × 40 features:
 *   - 64 workers  → handles ~640 concurrent Discord API calls → good for 3k active guilds
 *   - 128 workers → handles ~1280 concurrent Discord API calls → good for 6k active guilds
 */
export const DEFAULT_WORKER_COUNT = 64;

/** Default global ticker interval in ms (checks for deferred wakeups) */
export const DEFAULT_TICK_INTERVAL_MS = 50;

/** Jobs older than this (ms) in the inflight HASH are considered orphaned on restart */
export const DEFAULT_VISIBILITY_TIMEOUT_MS = 5 * 60 * 1_000; // 5 minutes

/** Max jobs a single worker drains from one queue per notification */
export const MAX_DRAIN_PER_NOTIFY = 32;

/** Worker BRPOP timeout in seconds (must be integer for Redis) */
export const BRPOP_TIMEOUT_SECONDS = 1;

/** Default job priority (middle of 1–10 scale) */
export const DEFAULT_PRIORITY = 5;

/** Default maximum retries on job failure */
export const DEFAULT_MAX_RETRIES = 3;

/** Wakeup ticker batch size — max queues to wake per tick */
export const WAKEUP_BATCH_SIZE = 100;

/**
 * Scale applied to the priority when composing a job's ZSET score.
 *
 * score = priority * PRIORITY_SCALE + arrivalSeq
 *
 * The priority occupies the high-order digits so a lower priority number
 * always sorts first, while the arrival sequence keeps jobs with the same
 * priority in FIFO order. Must match PRIORITY_SCALE in scripts/enqueue.lua.
 */
export const PRIORITY_SCALE = 1_000_000_000_000;
