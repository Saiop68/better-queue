import type { RedisOptions } from 'ioredis';

// ── Queue configuration ───────────────────────────────────────────────────────

/** Options for creating a DistributedLimiter instance. */
export interface LimiterOptions {
  /**
   * Unique identifier for this queue (e.g. a tenant or shard ID).
   * Used as the Redis key namespace: `bq:{id}:*`.
   */
  readonly id: string;

  /**
   * Maximum number of jobs that can run concurrently in this queue.
   * @default 1
   */
  readonly maxConcurrent?: number;

  /**
   * Minimum delay between the start of consecutive jobs, in milliseconds.
   * Equivalent to Bottleneck's `minTime`.
   * @default 0
   */
  readonly minTime?: number;

  /**
   * Initial token count for the reservoir (token bucket).
   * Jobs consume one token each. When empty, jobs are held until a refresh.
   * Set to `undefined` or omit to disable reservoir limiting.
   */
  readonly reservoir?: number;

  /**
   * Interval (ms) at which the reservoir is refilled.
   * Requires `reservoir` and `reservoirRefreshAmount` to be set.
   * @default 0 (disabled)
   */
  readonly reservoirRefreshInterval?: number;

  /**
   * Number of tokens added per refresh interval.
   * @default 0 (disabled)
   */
  readonly reservoirRefreshAmount?: number;

  /**
   * Interval (ms) at which the reservoir is *incremented*.
   *
   * Unlike `reservoirRefreshAmount` (which RESETS the reservoir to a fixed
   * value), `reservoirIncreaseAmount` ADDS tokens on every interval — the
   * classic "tokens trickle back over time" behaviour.
   *
   * Requires `reservoir` to be set. Cannot be combined with
   * `reservoirRefreshInterval` — the refresh (reset) wins if both are set.
   * @default 0 (disabled)
   */
  readonly reservoirIncreaseInterval?: number;

  /**
   * Number of tokens ADDED to the reservoir on every increase interval.
   * @default 0 (disabled)
   */
  readonly reservoirIncreaseAmount?: number;

  /**
   * Maximum value the reservoir may reach when using `reservoirIncreaseAmount`.
   * `0` (or omitted) means unbounded.
   * @default 0 (unbounded)
   */
  readonly reservoirIncreaseMaximum?: number;

  /**
   * Default job priority for this limiter (1 = highest, 10 = lowest).
   * Individual `schedule()` calls can override this.
   * @default 5
   */
  readonly priority?: number;

  /**
   * Redis key TTL in seconds. Keys for inactive queues expire automatically.
   * This prevents accumulation of stale data.
   * @default 86400 (24 hours)
   */
  readonly ttl?: number;

  /**
   * Whether to retry failed jobs.
   * @default true
   */
  readonly retryOnFailure?: boolean;

  /**
   * Maximum number of retry attempts per job.
   * @default 3
   */
  readonly maxRetries?: number;
}

/** Resolved (fully-defaulted) limiter configuration stored in Redis. */
export interface ResolvedLimiterOptions {
  readonly id: string;
  readonly maxConcurrent: number;
  readonly minTime: number;
  readonly reservoir: number; // -1 means unlimited
  readonly reservoirRefreshInterval: number;
  readonly reservoirRefreshAmount: number;
  readonly reservoirIncreaseInterval: number;
  readonly reservoirIncreaseAmount: number;
  readonly reservoirIncreaseMaximum: number;
  readonly priority: number;
  readonly ttl: number;
  readonly retryOnFailure: boolean;
  readonly maxRetries: number;
}

// ── Job types ─────────────────────────────────────────────────────────────────

/** Stored in Redis ZSET. Must be serializable to JSON string. */
export interface JobPayload {
  /** UUID generated at schedule time. Used to look up the in-memory closure. */
  readonly id: string;
  /** Priority 1–10 (lower = higher priority). Biases the ZSET score. */
  readonly priority: number;
  /** Max retries allowed for this job. */
  readonly maxRetries: number;
  /** Current retry attempt (0 = first attempt). */
  readonly retryCount: number;
  /** Unix timestamp (ms) when job was enqueued. Used for crash recovery. */
  readonly enqueuedAt: number;
  /**
   * Id of the WorkerPool that scheduled this job.
   *
   * The executable function is a JS closure and therefore lives in the
   * scheduling process only. The owner id lets a worker that picks up a
   * foreign job tell "another live process owns this" (requeue it) from
   * "the owner is gone" (orphan — drop it).
   */
  readonly owner: string;
}

/** In-memory record stored in WorkerPool.pendingJobs Map — exists only while job is pending/running. */
export interface PendingJob {
  /** The async function to execute. */
  readonly fn: () => Promise<unknown>;
  /** Resolve the user-facing Promise. */
  readonly resolve: (value: unknown) => void;
  /** Reject the user-facing Promise. */
  readonly reject: (reason: unknown) => void;
  /** Max retries (mirrored from payload for retry logic). */
  readonly maxRetries: number;
  /** Unix ms when this entry was inserted — used by orphan sweeper. */
  readonly enqueuedAt: number;
  /** Queue this job belongs to — used to route pool events back to the limiter. */
  readonly queueId: string;
}

// ── Schedule options ──────────────────────────────────────────────────────────

/** Per-job scheduling options passed to `limiter.schedule()`. */
export interface ScheduleOptions {
  /**
   * Job priority (1 = highest, 10 = lowest).
   * Overrides the limiter's default priority.
   */
  priority?: number;

  /**
   * Schedule the job to start at or after this Unix timestamp (ms).
   * Defaults to immediate scheduling.
   */
  scheduleAt?: number;

  /**
   * Override max retries for this specific job.
   */
  maxRetries?: number;
}

// ── Worker pool options ───────────────────────────────────────────────────────

/** Options for configuring the global WorkerPool. */
export interface WorkerPoolOptions {
  /** ioredis connection options. */
  readonly redis: RedisOptions;
  /** Number of concurrent workers. @default 4 */
  readonly workers?: number;
  /** Global ticker interval in ms (for deferred wakeups). @default 50 */
  readonly tickIntervalMs?: number;
  /** Jobs older than this (ms) in inflight are re-queued on startup. @default 300000 */
  readonly visibilityTimeoutMs?: number;
  /**
   * How long (ms) a pendingJobs entry may live before the sweeper rejects it
   * as orphaned. Set to 0 to disable the sweeper. @default 300000 (5 min)
   */
  readonly pendingJobTimeoutMs?: number;
}

// ── Dequeue result ────────────────────────────────────────────────────────────

export type DequeueStatus = 'ok' | 'empty' | 'blocked' | 'concurrent_full' | 'rate_limited';

export interface DequeueResult {
  readonly status: DequeueStatus;
  readonly payload: string;
  /**
   * Unix ms when the caller should retry.
   *
   * - `rate_limited`: when minTime / the reservoir allows the next job to run.
   * - `empty`: when the earliest *pending* (not-yet-due) job becomes eligible.
   * - `blocked`: the queue holds jobs owned by other processes; retry shortly.
   * - `concurrent_full`: every concurrency slot is taken — possibly by a job
   *   owned by another process; retry shortly.
   * - `0`: nothing pending — no retry needed.
   */
  readonly nextEligibleMs: number;
}

// ── Observability ─────────────────────────────────────────────────────────────

export interface QueueStats {
  /** Number of jobs waiting in the queue. */
  readonly queueSize: number;
  /** Number of jobs currently executing. */
  readonly runningCount: number;
  /** Current token count, or `null` if reservoir is not configured. */
  readonly reservoir: number | null;
}

// ── Events ────────────────────────────────────────────────────────────────────

export interface LimiterEvents {
  /** Emitted when a job starts executing. */
  executing: [jobId: string];
  /** Emitted when a job completes successfully. */
  done: [jobId: string];
  /** Emitted when a job fails (after all retries). */
  failed: [jobId: string, error: unknown];
  /** Emitted when a job is retried. */
  retry: [jobId: string, attempt: number];
  /** Emitted when the queue is drained (no more pending or running jobs). */
  drained: [];
}
