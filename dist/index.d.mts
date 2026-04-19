import Redis, { RedisOptions } from 'ioredis';
import { EventEmitter } from 'node:events';

/** Options for creating a DistributedLimiter instance. */
interface LimiterOptions {
    /**
     * Unique identifier for this queue (e.g. Discord guild ID).
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
interface ResolvedLimiterOptions {
    readonly id: string;
    readonly maxConcurrent: number;
    readonly minTime: number;
    readonly reservoir: number;
    readonly reservoirRefreshInterval: number;
    readonly reservoirRefreshAmount: number;
    readonly priority: number;
    readonly ttl: number;
    readonly retryOnFailure: boolean;
    readonly maxRetries: number;
}
/** Stored in Redis ZSET. Must be serializable to JSON string. */
interface JobPayload {
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
}
/** In-memory record stored in WorkerPool.pendingJobs Map — exists only while job is pending/running. */
interface PendingJob {
    /** The async function to execute. */
    readonly fn: () => Promise<unknown>;
    /** Resolve the user-facing Promise. */
    readonly resolve: (value: unknown) => void;
    /** Reject the user-facing Promise. */
    readonly reject: (reason: unknown) => void;
    /** Max retries (mirrored from payload for retry logic). */
    readonly maxRetries: number;
}
/** Per-job scheduling options passed to `limiter.schedule()`. */
interface ScheduleOptions {
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
/** Options for configuring the global WorkerPool. */
interface WorkerPoolOptions {
    /** ioredis connection options. */
    readonly redis: RedisOptions;
    /** Number of concurrent workers. @default 4 */
    readonly workers?: number;
    /** Global ticker interval in ms (for deferred wakeups). @default 50 */
    readonly tickIntervalMs?: number;
    /** Jobs older than this (ms) in inflight are re-queued on startup. @default 300000 */
    readonly visibilityTimeoutMs?: number;
}
type DequeueStatus = 'ok' | 'empty' | 'concurrent_full' | 'rate_limited';
interface DequeueResult {
    readonly status: DequeueStatus;
    readonly payload: string;
    /** Unix ms when the caller should retry. Only set for `rate_limited`. */
    readonly nextEligibleMs: number;
}
interface QueueStats {
    /** Number of jobs waiting in the queue. */
    readonly queueSize: number;
    /** Number of jobs currently executing. */
    readonly runningCount: number;
    /** Current token count, or `null` if reservoir is not configured. */
    readonly reservoir: number | null;
}
interface LimiterEvents {
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

/**
 * WorkerPool — the engine of BetterQueue.
 *
 * A single, globally-shared pool of workers that pull jobs from Redis and
 * execute them. There is ONE worker pool per Node.js process.
 *
 * Workers use BRPOP to block-wait for notifications. A single global ticker
 * handles deferred wakeups (for minTime / reservoir rate limiting), replacing
 * the need for any per-limiter timers.
 */
declare class WorkerPool extends EventEmitter {
    /** Shared command connection (non-blocking) */
    readonly commandClient: Redis;
    /**
     * In-memory map of job closures. The ONLY mutable in-process state.
     * Entry lifecycle: added in schedule() → deleted in executeJob() or handleJobError().
     * Size is bounded by the number of concurrently running + pending-but-not-yet-started jobs.
     */
    readonly pendingJobs: Map<string, PendingJob>;
    readonly defaultTtl: number;
    private readonly workers;
    private readonly redisOptions;
    private readonly tickIntervalMs;
    private readonly visibilityTimeoutMs;
    private tickerHandle;
    private stopped;
    private constructor();
    /**
     * Create and start the global WorkerPool.
     * Must be called once before any `DistributedLimiter` is created.
     *
     * @example
     * const pool = await WorkerPool.create({ redis: { host: 'localhost', port: 6379 } });
     */
    static create(options: WorkerPoolOptions): Promise<WorkerPool>;
    /** Returns the active global pool, or throws if not initialized. */
    static getInstance(): WorkerPool;
    private start;
    /**
     * Gracefully stop the pool.
     * Waits for all in-flight jobs to complete, then closes connections.
     */
    stop(): Promise<void>;
    /**
     * Run crash recovery for a queue — re-enqueue orphaned inflight jobs.
     * Called automatically by DistributedLimiter on first use.
     */
    recoverQueue(queueId: string, ttl: number): Promise<number>;
    private startTicker;
}

/**
 * DistributedLimiter — the public API of BetterQueue.
 *
 * Each instance represents a single rate-limited queue identified by a unique
 * `id`. Queue state (concurrency slots, reservoir tokens, job list) lives
 * entirely in Redis — the JS object itself holds only configuration.
 *
 * Multiple `DistributedLimiter` instances with the same `id` (across the same
 * or different processes) share the same logical queue via Redis.
 *
 * @example
 * ```ts
 * const pool = await WorkerPool.create({ redis: { host: 'localhost' } });
 *
 * const limiter = new DistributedLimiter({
 *   id: 'guild:123456789',
 *   maxConcurrent: 2,
 *   minTime: 250,
 *   reservoir: 10,
 *   reservoirRefreshInterval: 1_000,
 *   reservoirRefreshAmount: 5,
 * });
 *
 * const result = await limiter.schedule(() => callDiscordApi());
 * ```
 */
declare class DistributedLimiter extends EventEmitter {
    private readonly opts;
    private readonly pool;
    private stopped;
    private metaSynced;
    private recoveryDone;
    constructor(options: LimiterOptions);
    /** The unique queue identifier. */
    get id(): string;
    /**
     * Schedule an async function to run under this limiter's rate limits.
     *
     * The function is enqueued in Redis and will be executed by a WorkerPool
     * worker when rate-limit conditions are satisfied. Returns a Promise that
     * resolves/rejects with the function's result.
     *
     * @param fn - The async function to execute.
     * @param options - Per-job scheduling options (priority, scheduleAt, etc.)
     * @returns A Promise that resolves with the function's return value.
     *
     * @example
     * const data = await limiter.schedule(() => fetch('/api/data').then(r => r.json()));
     */
    schedule<T>(fn: () => Promise<T>, options?: ScheduleOptions): Promise<T>;
    /**
     * Returns all queue statistics in a single atomic Redis read.
     */
    getStats(): Promise<QueueStats>;
    /** Number of jobs waiting to be executed. */
    getQueueSize(): Promise<number>;
    /** Number of jobs currently executing. */
    getRunningCount(): Promise<number>;
    /**
     * Current reservoir token count.
     * Returns `null` if reservoir is not configured for this limiter.
     */
    getReservoir(): Promise<number | null>;
    /**
     * Update limiter settings. Changes are written to Redis and take effect on
     * the next dequeue attempt.
     */
    updateSettings(patch: Partial<Omit<LimiterOptions, 'id'>>): Promise<void>;
    /**
     * Wait until all currently queued and running jobs finish.
     * Polls Redis every 100ms.
     */
    drain(): Promise<void>;
    /**
     * Stop accepting new jobs. Does NOT drain existing jobs.
     * Call `drain()` first if you want to wait for pending jobs.
     */
    stop(): void;
    /**
     * Lazily synchronize queue meta to Redis and run crash recovery.
     * Only runs once per lifteime of the DistributedLimiter instance.
     */
    private ensureInitialized;
    /** Write resolved options to the Redis HASH so Lua scripts can read them. */
    private syncMeta;
}
interface DistributedLimiter {
    on<K extends keyof LimiterEvents>(event: K, listener: (...args: LimiterEvents[K]) => void): this;
    emit<K extends keyof LimiterEvents>(event: K, ...args: LimiterEvents[K]): boolean;
}

/**
 * src/LimiterManager.ts
 *
 * Drop-in replacement for the Bottleneck-based LimiterManager.
 *
 * API is intentionally kept 100% compatible with the original so you only need
 * to change the import path in your bot. No other code changes required.
 *
 * Internals use BetterQueue (Redis-backed, zero per-limiter timers) instead of
 * Bottleneck, which eliminates memory leaks and per-guild setInterval overhead.
 *
 * @example
 * // In your bot — just change this one line:
 * import limiterManager from 'better-queue/LimiterManager';
 * // Everything else (registerFeatureConfig, getLimiter, schedule) stays the same.
 */

/**
 * Rate limit configuration for a specific feature (e.g. 'guild-member-add').
 * Mirrors Bottleneck.ConstructorOptions for compatibility.
 */
interface FeatureConfig {
    /** Max concurrent jobs executing at once (per guild queue). */
    maxConcurrent?: number;
    /** Token bucket size — max burst. -1 or undefined = unlimited. */
    reservoir?: number;
    /** Tokens added on each refresh cycle. */
    reservoirRefreshAmount?: number;
    /** How often the reservoir refills (ms). 0 = disabled. */
    reservoirRefreshInterval?: number;
    /** Minimum delay between job starts (ms). 0 = no minimum. */
    minTime?: number;
    /**
     * Per-job timeout in ms. Jobs that take longer will be rejected with a timeout error.
     * Equivalent to Bottleneck's `timeout`. Default: 30 000.
     *
     * IMPORTANT: When using LimiterManager, set WorkerPool's `visibilityTimeoutMs` to a
     * value >= this timeout. Otherwise crash-recovery can re-enqueue still-running jobs.
     *
     * @example
     * await WorkerPool.create({
     *   redis: { host: 'localhost' },
     *   visibilityTimeoutMs: 3_600_000, // match your longest job timeout
     * });
     */
    timeout?: number;
    /** Redis key TTL (seconds). Default: 86400 (24h). */
    ttl?: number;
    /** Max retry attempts on failure. Default: 3. */
    maxRetries?: number;
    /**
     * Bottleneck compat shim — ignored by BetterQueue.
     * BetterQueue always tracks status via Redis; there is no overhead to disable.
     */
    trackDoneStatus?: boolean;
}
/** Schedule options passed as the first argument — matches Bottleneck's API. */
interface ScheduleJobOptions {
    /** Optional job identifier (for deduplication / logging). */
    id?: string;
    /** Lower number = higher priority. Default: 5. */
    priority?: number;
    /** Schedule this job to start no earlier than this Unix-ms timestamp. */
    scheduleAt?: number;
}
/**
 * Thin wrapper around DistributedLimiter that exposes the same `.schedule()`
 * overload Bottleneck uses, so all existing call-sites work without changes.
 */
declare class LimiterWrapper {
    private readonly _limiter;
    private readonly _timeout;
    constructor(limiter: DistributedLimiter, timeout: number);
    /** `limiter.schedule(fn)` */
    schedule<T>(fn: () => Promise<T>): Promise<T>;
    /** `limiter.schedule({ id, priority }, fn)` */
    schedule<T>(options: ScheduleJobOptions, fn: () => Promise<T>): Promise<T>;
    /**
     * Returns job counts in Bottleneck's format.
     * Useful for code that calls `limiter.counts()` for health checks.
     */
    counts(): Promise<{
        RECEIVED: number;
        QUEUED: number;
        RUNNING: number;
        EXECUTING: number;
        DONE: number;
    }>;
    /** Direct access to the underlying DistributedLimiter if needed. */
    get inner(): DistributedLimiter;
    /** Wait until this guild's queue is fully drained (all jobs done). */
    drain(): Promise<void>;
    /** Stop accepting new jobs for this limiter. */
    stop(): void;
}
/**
 * LimiterManager — singleton that owns all guild×feature rate limiters.
 *
 * Replaces the original Bottleneck-based version with BetterQueue:
 *   • Zero per-limiter timers (one global ticker for the whole process)
 *   • All queue state in Redis — limiters are tiny JS objects (~50 bytes)
 *   • Handles 100k+ concurrent guild queues without memory growth
 *   • Limiters are created lazily and cached in an LRU
 */
declare class LimiterManager {
    private static _instance;
    /**
     * Guild×feature limiter cache.
     * Key: `${guildId}::${feature}`
     * Large max to avoid evicting active guild limiters.
     */
    private readonly _limiterCache;
    /**
     * Per-feature default configs registered via `registerFeatureConfig()`.
     */
    private readonly _featureConfigs;
    private constructor();
    static getInstance(): LimiterManager;
    /**
     * Register the default rate-limit config for a feature.
     * Call once at startup (e.g. in the queue class constructor).
     *
     * @example
     * limiterManager.registerFeatureConfig('guild-member-add', {
     *   maxConcurrent: 1,
     *   reservoir: 10,
     *   reservoirRefreshAmount: 10,
     *   reservoirRefreshInterval: 10_000,
     *   minTime: 1_000,
     *   timeout: 3_600_000,
     * });
     */
    registerFeatureConfig(feature: string, config: Partial<FeatureConfig>): void;
    /**
     * Get (or lazily create) a per-guild, per-feature rate limiter.
     *
     * @example
     * const limiter = limiterManager.getLimiter(guildId, 'guild-member-add');
     * await limiter.schedule({ id: `${guildId}-${memberId}` }, () => doWork());
     */
    getLimiter(guildId: string, feature: string): LimiterWrapper;
    /**
     * Stop and remove all cached limiters.
     * Call on graceful shutdown.
     */
    clearAll(): Promise<void>;
    /**
     * Cache utilization stats — matches original LimiterManager.getStats() format.
     */
    getStats(): {
        limiterCache: {
            size: number;
            max: number;
            usage: string;
            ttl: number;
        };
        featureConfigs: {
            size: number;
            max: number;
            usage: string;
            ttl: number;
        };
    };
    /** Health report — matches original LimiterManager.getCacheUsage() format. */
    getCacheUsage(): {
        status: string;
        limiterCount: number;
        maxLimiters: number;
        isHealthy: boolean;
        recommendation: string;
    };
    /**
     * Drain a specific guild+feature queue — wait until all pending jobs complete.
     */
    drainGuild(guildId: string, feature: string): Promise<void>;
}
declare const limiterManager: LimiterManager;

/**
 * Redis key schema for BetterQueue.
 * All keys are namespaced under `bq:` to avoid collisions.
 */
declare const KEYS: {
    /** ZSET — pending jobs, score = earliest allowed start time (ms) */
    readonly jobs: (id: string) => string;
    /** SET — job IDs of currently executing jobs */
    readonly running: (id: string) => string;
    /** HASH — jobId → full payload, for crash recovery */
    readonly inflight: (id: string) => string;
    /** HASH — queue configuration (maxConcurrent, minTime, etc.) */
    readonly meta: (id: string) => string;
    /** STRING (int) — current token count for reservoir */
    readonly reservoir: (id: string) => string;
    /** STRING (ms) — timestamp of last job dequeue */
    readonly lastRun: (id: string) => string;
    /** STRING (ms) — timestamp of last reservoir refresh */
    readonly lastRefresh: (id: string) => string;
    /** LIST — worker notification channel (BRPOP) */
    readonly notify: "bq:workers:notify";
    /** ZSET — scheduled worker wakeups, score = wakeup time (ms) */
    readonly wakeup: "bq:global:wakeup";
};

/** Base class for all BetterQueue errors. */
declare class BetterQueueError extends Error {
    constructor(message: string);
}
/** Thrown when a job is dropped due to exceeding max retries. */
declare class JobMaxRetriesError extends BetterQueueError {
    readonly jobId: string;
    readonly attempts: number;
    readonly cause: unknown;
    constructor(jobId: string, attempts: number, cause: unknown);
}
/** Thrown when schedule() is called after the limiter has been stopped. */
declare class LimiterStoppedError extends BetterQueueError {
    readonly queueId: string;
    constructor(queueId: string);
}
/** Thrown when the WorkerPool has not been initialized before creating a limiter. */
declare class WorkerPoolNotInitializedError extends BetterQueueError {
    constructor();
}
/** Thrown when a Redis command fails unexpectedly. */
declare class RedisCommandError extends BetterQueueError {
    constructor(command: string, cause: unknown);
}

export { BetterQueueError, type DequeueResult, type DequeueStatus, DistributedLimiter, type FeatureConfig, JobMaxRetriesError, type JobPayload, KEYS, type LimiterEvents, LimiterManager, type LimiterOptions, LimiterStoppedError, LimiterWrapper, type PendingJob, type QueueStats, RedisCommandError, type ResolvedLimiterOptions, type ScheduleJobOptions, type ScheduleOptions, WorkerPool, WorkerPoolNotInitializedError, type WorkerPoolOptions, limiterManager };
