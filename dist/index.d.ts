import Redis from 'ioredis';
import { EventEmitter } from 'node:events';
import { P as PendingJob, W as WorkerPoolOptions } from './LimiterManager-DUS2PFal.js';
export { D as DequeueResult, a as DequeueStatus, b as DistributedLimiter, F as FeatureConfig, J as JobPayload, L as LimiterEvents, c as LimiterManager, d as LimiterOptions, e as LimiterWrapper, Q as QueueStats, R as ResolvedLimiterOptions, S as ScheduleJobOptions, f as ScheduleOptions, l as limiterManager } from './LimiterManager-DUS2PFal.js';

/**
 * WorkerPool — the engine of BetterQueue.
 *
 * A single, globally-shared pool of workers that pull jobs from Redis and
 * execute them. There is ONE worker pool per Node.js process.
 *
 * Workers use BRPOP to block-wait for notifications. A single global ticker
 * handles deferred wakeups (for minTime / reservoir / scheduleAt rate
 * limiting), replacing the need for any per-limiter timers.
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
    /**
     * Per-queue event subscribers.
     *
     * A `DistributedLimiter` subscribes to the pool's job events for its own
     * queue id. Keeping them in a map (instead of adding a listener per
     * limiter to this shared emitter) means the emitter's listener count stays
     * constant no matter how many limiters exist.
     */
    private readonly queueSubscribers;
    readonly defaultTtl: number;
    /**
     * Unique id of this WorkerPool instance (one per process).
     *
     * Jobs are stamped with the id of the pool that scheduled them. Because a
     * job's executable function is a JS closure living in the scheduling
     * process, only that process may run it — so the id is what makes
     * cross-process coordination safe.
     */
    readonly poolId: string;
    private readonly workers;
    private readonly redisOptions;
    private readonly tickIntervalMs;
    private readonly visibilityTimeoutMs;
    private readonly pendingJobTimeoutMs;
    private tickerHandle;
    private sweeperHandle;
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
    /**
     * Subscribe to job events for a single queue id.
     *
     * Returns an unsubscribe function.
     */
    subscribeQueue(queueId: string, handlers: {
        executing?: (jobId: string) => void;
        done?: (jobId: string) => void;
        failed?: (jobId: string, error: unknown) => void;
        retry?: (jobId: string, attempt: number) => void;
    }): () => void;
    /** Notify the subscribers of a queue that a job started executing. */
    dispatchExecuting(queueId: string, jobId: string): void;
    /** Notify the subscribers of a queue that a job completed. */
    dispatchDone(queueId: string, jobId: string): void;
    /** Notify the subscribers of a queue that a job failed. */
    dispatchFailed(queueId: string, jobId: string, error: unknown): void;
    /** Notify the subscribers of a queue that a job is being retried. */
    dispatchRetry(queueId: string, jobId: string, attempt: number): void;
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
    /** Refresh this pool's liveness heartbeat in Redis. */
    private heartbeat;
    /**
     * Periodically scans `pendingJobs` and rejects any entry that has been
     * waiting longer than `pendingJobTimeoutMs`. This catches jobs whose Redis
     * BRPOP notification was lost (e.g. Redis restart, network blip) and would
     * otherwise hold their closures in memory forever.
     */
    private startSweeper;
}

/**
 * Redis key schema for BetterQueue.
 * All keys are namespaced under `bq:` to avoid collisions.
 */
declare const KEYS: {
    /** ZSET — ready jobs, score = priority * 1e12 + arrival seq */
    readonly jobs: (id: string) => string;
    /** ZSET — jobs not yet due (scheduleAt / retry backoff), score = due time (ms) */
    readonly delayed: (id: string) => string;
    /** SET — job IDs of currently executing jobs */
    readonly running: (id: string) => string;
    /** HASH — jobId → full payload, for crash recovery */
    readonly inflight: (id: string) => string;
    /** HASH — queue configuration (maxConcurrent, minTime, etc.) + arrival counter */
    readonly meta: (id: string) => string;
    /** STRING (int) — current token count for reservoir */
    readonly reservoir: (id: string) => string;
    /** STRING (ms) — timestamp of last job dequeue */
    readonly lastRun: (id: string) => string;
    /** STRING (ms) — timestamp of last reservoir refresh */
    readonly lastRefresh: (id: string) => string;
    /** STRING (ms) — timestamp of last reservoir increase */
    readonly lastIncrease: (id: string) => string;
    /**
     * LIST — per-pool notification channel.
     *
     * Each WorkerPool listens on its OWN channel so that a notification for
     * pool A is never consumed by pool B. Without this, a second process would
     * steal the first process's notification and its job would never run.
     */
    readonly notifyPool: (poolId: string) => string;
    /** ZSET — scheduled worker wakeups, score = wakeup time (ms) */
    readonly wakeup: "bq:global:wakeup";
    /** ZSET — live WorkerPool registry, score = last heartbeat (ms) */
    readonly pools: "bq:pools";
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

export { BetterQueueError, JobMaxRetriesError, KEYS, LimiterStoppedError, PendingJob, RedisCommandError, WorkerPool, WorkerPoolNotInitializedError, WorkerPoolOptions };
