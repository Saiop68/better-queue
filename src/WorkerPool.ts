import type Redis from 'ioredis';
import type { RedisConnection } from './RedisClient';
import { EventEmitter } from 'node:events';
import { randomUUID } from 'node:crypto';
import { createRedisClient, createBlockingClient } from './RedisClient';
import { KEYS, DEFAULT_WORKER_COUNT, DEFAULT_TICK_INTERVAL_MS, DEFAULT_VISIBILITY_TIMEOUT_MS, BRPOP_TIMEOUT_SECONDS, MAX_DRAIN_PER_NOTIFY, WAKEUP_BATCH_SIZE, POOL_LIVE_WINDOW_MS } from './constants';
import { WorkerPoolNotInitializedError, JobMaxRetriesError, OrphanedJobError } from './errors';
import type { WorkerPoolOptions, PendingJob, JobPayload, DequeueResult } from './types';

/** Default ms before an unexecuted pendingJobs entry is considered orphaned. */
const DEFAULT_PENDING_JOB_TIMEOUT_MS = 300_000; // 5 minutes
/** How often the orphan sweeper runs. */
const SWEEPER_INTERVAL_MS = 30_000; // 30 seconds
/**
 * How many entries of the ready ZSET a dequeue call may inspect while looking
 * for a job owned by this pool. Bounds the cost of the owner-aware scan.
 */
const DEQUEUE_SCAN_WINDOW = 256;

// ── Singleton guard ────────────────────────────────────────────────────────
let globalPool: WorkerPool | null = null;

// ── Helpers ────────────────────────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseDequeueResult(raw: [string, string, string]): DequeueResult {
  return {
    status: raw[0] as DequeueResult['status'],
    payload: raw[1] ?? '',
    nextEligibleMs: parseInt(raw[2] ?? '0', 10),
  };
}

// ── Worker (one per BRPOP connection) ──────────────────────────────────────

class Worker {
  private readonly id: number;
  private readonly blockingConn: Redis;
  private readonly pool: WorkerPool;
  private running = false;

  constructor(id: number, redisOptions: RedisConnection, pool: WorkerPool) {
    this.id      = id;
    this.pool    = pool;
    this.blockingConn = createBlockingClient(redisOptions);
  }

  start(): void {
    this.running = true;
    void this.loop();
  }

  async stop(): Promise<void> {
    this.running = false;
    await this.blockingConn.quit();
  }

  private async loop(): Promise<void> {
    while (this.running) {
      try {
        // BRPOP blocks until a queue ID arrives or times out (1s).
        // Each pool has its OWN channel so that another process can never
        // consume this pool's notification.
        const result = await this.blockingConn.brpop(
          KEYS.notifyPool(this.pool.poolId),
          BRPOP_TIMEOUT_SECONDS,
        );

        if (!result) continue; // timeout — just loop back

        const queueId = result[1];
        if (queueId) await this.drainQueue(queueId);
      } catch (err) {
        if (this.running) {
          // Transient error — back off briefly to prevent tight error loops
          await sleep(200);
        }
      }
    }
  }

  /**
   * Drain as many jobs as possible from a queue in one burst.
   * Stops when the queue is empty, rate-limited, or max drain count reached.
   */
  private async drainQueue(queueId: string): Promise<void> {
    for (let i = 0; i < MAX_DRAIN_PER_NOTIFY; i++) {
      const result = await this.tryDequeue(queueId);
      if (!result) break;

      const { status, payload, nextEligibleMs } = result;

      if (status === 'ok' && payload) {
        await this.executeJob(queueId, payload);
        continue; // immediately try next job in same queue
      }

      // `rate_limited` → minTime / reservoir says when the next job may start.
      // `empty`        → a job exists but is not due yet (scheduleAt / backoff);
      //                   nextEligibleMs is when it becomes eligible.
      // `blocked`      → the queue holds jobs owned by other processes; look
      //                   again shortly.
      // `concurrent_full` → every concurrency slot is taken — possibly by a job
      //                   owned by ANOTHER process. We cannot know when that
      //                   job finishes, so look again shortly.
      // All four need a wakeup, otherwise the job would sit in Redis forever
      // (this used to be the cross-process stall: `concurrent_full` reported
      // nextEligibleMs = 0 and was never scheduled).
      if (
        (status === 'rate_limited' ||
          status === 'empty' ||
          status === 'blocked' ||
          status === 'concurrent_full') &&
        nextEligibleMs > 0
      ) {
        // Schedule a wakeup — the ticker will re-notify workers at the right time
        await this.scheduleWakeup(queueId, nextEligibleMs);
      }

      // For all non-ok statuses (empty, blocked, concurrent_full, rate_limited),
      // stop draining
      break;
    }
  }

  private async tryDequeue(queueId: string): Promise<DequeueResult | null> {
    try {
      const raw = await this.pool.commandClient.bqDequeue(
        KEYS.jobs(queueId),
        KEYS.delayed(queueId),
        KEYS.running(queueId),
        KEYS.inflight(queueId),
        KEYS.meta(queueId),
        KEYS.reservoir(queueId),
        KEYS.lastRun(queueId),
        KEYS.lastRefresh(queueId),
        KEYS.lastIncrease(queueId),
        String(Date.now()),
        String(this.pool.defaultTtl),
        this.pool.poolId,
        String(DEQUEUE_SCAN_WINDOW),
      );
      return parseDequeueResult(raw);
    } catch (err) {
      process.stderr.write(`[BetterQueue] dequeue error for queue "${queueId}": ${String(err)}\n`);
      return null;
    }
  }

  private async executeJob(queueId: string, payloadJson: string): Promise<void> {
    let job: JobPayload;
    try {
      job = JSON.parse(payloadJson) as JobPayload;
    } catch {
      // Corrupted payload — skip silently
      return;
    }

    const pending = this.pool.pendingJobs.get(job.id);

    if (!pending) {
      // The job's closure is not in this process.
      //
      // With owner-aware dequeue this is only reachable for a job whose owner
      // is gone (the owning process crashed): the closure can never be
      // recovered, so the job is an orphan — clean up its Redis state.
      await this.completeJob(queueId, job.id);
      return;
    }

    this.pool.dispatchExecuting(queueId, job.id);

    try {
      const value = await pending.fn();
      // Delete from map BEFORE resolving to free memory ASAP
      this.pool.pendingJobs.delete(job.id);
      await this.completeJob(queueId, job.id);
      pending.resolve(value);
      this.pool.dispatchDone(queueId, job.id);
    } catch (err: unknown) {
      await this.handleJobError(queueId, job, pending, err);
    }
  }

  /**
   * Handle a failed job: retry with exponential backoff, or reject the
   * caller's promise once the retry budget is exhausted.
   */
  private async handleJobError(
    queueId: string,
    job: JobPayload,
    pending: PendingJob,
    err: unknown,
  ): Promise<void> {
    const attempt = job.retryCount + 1;

    if (job.retryCount < job.maxRetries) {
      // Exponential backoff: 1s, 2s, 4s, 8s… capped at 30s
      const delayMs = Math.min(1_000 * 2 ** job.retryCount, 30_000);
      const retryPayload: JobPayload = { ...job, retryCount: attempt };

      this.pool.dispatchRetry(queueId, job.id, attempt);

      // Keep fn + callbacks in map — they'll be used on retry.
      // The retry is re-enqueued with a *relative* delay so the Redis server
      // clock (not the client clock) decides when it becomes due.
      try {
        await this.pool.commandClient.bqEnqueue(
          KEYS.jobs(queueId),
          KEYS.delayed(queueId),
          KEYS.meta(queueId),
          KEYS.wakeup,
          KEYS.pools,
          JSON.stringify(retryPayload),
          '0',       // scheduleAt — absolute, not used for retries
          String(delayMs),
          String(job.priority),
          String(this.pool.defaultTtl),
          queueId,
          String(POOL_LIVE_WINDOW_MS),
        );
      } catch {
        // If re-enqueue fails, fall through to failure handling
        this.pool.pendingJobs.delete(job.id);
        const retryErr = new JobMaxRetriesError(job.id, attempt, err);
        pending.reject(retryErr);
        this.pool.dispatchFailed(queueId, job.id, retryErr);
        return;
      }

      await this.completeJob(queueId, job.id);
    } else {
      // Max retries exhausted
      this.pool.pendingJobs.delete(job.id);
      await this.completeJob(queueId, job.id);
      const finalErr = new JobMaxRetriesError(job.id, attempt, err);
      pending.reject(finalErr);
      this.pool.dispatchFailed(queueId, job.id, finalErr);
    }
  }

  private async completeJob(queueId: string, jobId: string): Promise<void> {
    try {
      await this.pool.commandClient.bqComplete(
        KEYS.running(queueId),
        KEYS.inflight(queueId),
        KEYS.jobs(queueId),
        KEYS.meta(queueId),        // for TTL refresh + idle cleanup signal
        KEYS.lastRun(queueId),     // deleted immediately when queue goes idle
        KEYS.delayed(queueId),
        KEYS.wakeup,
        KEYS.pools,
        jobId,
        queueId,
        String(this.pool.defaultTtl),
        this.pool.poolId,
        String(POOL_LIVE_WINDOW_MS),
      );
    } catch {
      // Best-effort — not fatal
    }
  }

  private async scheduleWakeup(queueId: string, atMs: number): Promise<void> {
    try {
      // ZADD bq:global:wakeup <atMs> "<poolId>|<queueId>"
      //
      // The member is scoped to this pool, so a queue can never accumulate
      // more than ONE pending wakeup per pool — the ZSET stays bounded no
      // matter how often a worker retries. The script keeps the EARLIEST
      // wakeup: if one is already scheduled for a later time, it is pulled
      // in; if one is scheduled sooner, it is left alone.
      //
      // The whole read-modify-write happens inside Lua, so two concurrent
      // wakeup requests for the same queue cannot race and push the earlier
      // time out.
      await this.pool.commandClient.bqWakeupSchedule(
        KEYS.wakeup,
        `${this.pool.poolId}|${queueId}`,
        String(atMs),
      );
    } catch {
      // Best-effort
    }
  }
}

// ── WorkerPool ─────────────────────────────────────────────────────────────

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
export class WorkerPool extends EventEmitter {
  /** Shared command connection (non-blocking) */
  readonly commandClient: Redis;

  /**
   * In-memory map of job closures. The ONLY mutable in-process state.
   * Entry lifecycle: added in schedule() → deleted in executeJob() or handleJobError().
   * Size is bounded by the number of concurrently running + pending-but-not-yet-started jobs.
   */
  readonly pendingJobs = new Map<string, PendingJob>();

  /**
   * Per-queue event subscribers.
   *
   * A `DistributedLimiter` subscribes to the pool's job events for its own
   * queue id. Keeping them in a map (instead of adding a listener per
   * limiter to this shared emitter) means the emitter's listener count stays
   * constant no matter how many limiters exist.
   */
  private readonly queueSubscribers = new Map<
    string,
    {
      executing: Set<(jobId: string) => void>;
      done: Set<(jobId: string) => void>;
      failed: Set<(jobId: string, error: unknown) => void>;
      retry: Set<(jobId: string, attempt: number) => void>;
    }
  >();

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

  private readonly workers: Worker[];
  private readonly redisOptions: RedisConnection;
  private readonly tickIntervalMs: number;
  private readonly visibilityTimeoutMs: number;
  private readonly pendingJobTimeoutMs: number;
  private tickerHandle: ReturnType<typeof setInterval> | null = null;
  private sweeperHandle: ReturnType<typeof setInterval> | null = null;
  private stopped = false;

  private constructor(options: WorkerPoolOptions) {
    super();

    const workerCount             = options.workers             ?? DEFAULT_WORKER_COUNT;
    this.tickIntervalMs           = options.tickIntervalMs      ?? DEFAULT_TICK_INTERVAL_MS;
    this.visibilityTimeoutMs      = options.visibilityTimeoutMs ?? DEFAULT_VISIBILITY_TIMEOUT_MS;
    this.pendingJobTimeoutMs      = options.pendingJobTimeoutMs ?? DEFAULT_PENDING_JOB_TIMEOUT_MS;
    this.defaultTtl               = 86_400;
    this.redisOptions             = options.redis;
    this.poolId                   = `${process.pid}-${randomUUID().slice(0, 8)}`;

    this.commandClient = createRedisClient(options.redis);

    this.workers = Array.from(
      { length: workerCount },
      (_, i) => new Worker(i, options.redis, this),
    );
  }

  /**
   * Create and start the global WorkerPool.
   * Must be called once before any `DistributedLimiter` is created.
   *
   * @example
   * const pool = await WorkerPool.create({ redis: { host: 'localhost', port: 6379 } });
   */
  static async create(options: WorkerPoolOptions): Promise<WorkerPool> {
    if (globalPool) return globalPool;

    const pool = new WorkerPool(options);
    await pool.start();
    globalPool = pool;
    return pool;
  }

  /** Returns the active global pool, or throws if not initialized. */
  static getInstance(): WorkerPool {
    if (!globalPool) throw new WorkerPoolNotInitializedError();
    return globalPool;
  }

  /**
   * Subscribe to job events for a single queue id.
   *
   * Returns an unsubscribe function.
   */
  subscribeQueue(
    queueId: string,
    handlers: {
      executing?: (jobId: string) => void;
      done?: (jobId: string) => void;
      failed?: (jobId: string, error: unknown) => void;
      retry?: (jobId: string, attempt: number) => void;
    },
  ): () => void {
    let entry = this.queueSubscribers.get(queueId);
    if (!entry) {
      entry = { executing: new Set(), done: new Set(), failed: new Set(), retry: new Set() };
      this.queueSubscribers.set(queueId, entry);
    }
    if (handlers.executing) entry.executing.add(handlers.executing);
    if (handlers.done) entry.done.add(handlers.done);
    if (handlers.failed) entry.failed.add(handlers.failed);
    if (handlers.retry) entry.retry.add(handlers.retry);

    return (): void => {
      const current = this.queueSubscribers.get(queueId);
      if (!current) return;
      if (handlers.executing) current.executing.delete(handlers.executing);
      if (handlers.done) current.done.delete(handlers.done);
      if (handlers.failed) current.failed.delete(handlers.failed);
      if (handlers.retry) current.retry.delete(handlers.retry);
      if (
        current.executing.size === 0 &&
        current.done.size === 0 &&
        current.failed.size === 0 &&
        current.retry.size === 0
      ) {
        this.queueSubscribers.delete(queueId);
      }
    };
  }

  /** Notify the subscribers of a queue that a job started executing. */
  dispatchExecuting(queueId: string, jobId: string): void {
    const entry = this.queueSubscribers.get(queueId);
    if (!entry) return;
    for (const fn of entry.executing) {
      try { fn(jobId); } catch { /* a bad listener must not break the worker */ }
    }
  }

  /** Notify the subscribers of a queue that a job completed. */
  dispatchDone(queueId: string, jobId: string): void {
    const entry = this.queueSubscribers.get(queueId);
    if (!entry) return;
    for (const fn of entry.done) {
      try { fn(jobId); } catch { /* ignore */ }
    }
  }

  /** Notify the subscribers of a queue that a job failed. */
  dispatchFailed(queueId: string, jobId: string, error: unknown): void {
    const entry = this.queueSubscribers.get(queueId);
    if (!entry) return;
    for (const fn of entry.failed) {
      try { fn(jobId, error); } catch { /* ignore */ }
    }
  }

  /** Notify the subscribers of a queue that a job is being retried. */
  dispatchRetry(queueId: string, jobId: string, attempt: number): void {
    const entry = this.queueSubscribers.get(queueId);
    if (!entry) return;
    for (const fn of entry.retry) {
      try { fn(jobId, attempt); } catch { /* ignore */ }
    }
  }

  private async start(): Promise<void> {
    for (const worker of this.workers) {
      worker.start();
    }

    // Register in the live-pool registry immediately so other processes can
    // notify us from the very first job.
    await this.heartbeat();

    this.startTicker();
    if (this.pendingJobTimeoutMs > 0) this.startSweeper();

    // Ensure pool is cleaned up on process exit so connections and closures
    // don't leak in long-running applications.
    const shutdown = (): void => { void this.stop(); };
    process.once('SIGINT',  shutdown);
    process.once('SIGTERM', shutdown);
  }

  /**
   * Gracefully stop the pool.
   * Waits for all in-flight jobs to complete, then closes connections.
   */
  async stop(): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;

    // Stop ticker
    if (this.tickerHandle) {
      clearInterval(this.tickerHandle);
      this.tickerHandle = null;
    }

    // Stop orphan sweeper
    if (this.sweeperHandle) {
      clearInterval(this.sweeperHandle);
      this.sweeperHandle = null;
    }

    // Reject any remaining orphaned pending jobs before closing connections
    for (const [jobId, pending] of this.pendingJobs) {
      pending.reject(new OrphanedJobError(jobId));
    }
    this.pendingJobs.clear();

    // Stop all workers
    await Promise.all(this.workers.map((w) => w.stop()));

    // Remove this pool from the live registry and drop its notification
    // channel, so other processes stop notifying it immediately instead of
    // waiting for the liveness window to expire.
    try {
      await this.commandClient.bqPoolUnregister(
        KEYS.pools,
        KEYS.notifyPool(this.poolId),
        this.poolId,
      );
    } catch {
      /* best-effort */
    }

    // Close command connection
    await this.commandClient.quit();

    if (globalPool === this) globalPool = null;
  }

  /**
   * Run crash recovery for a queue — re-enqueue orphaned inflight jobs.
   * Called automatically by DistributedLimiter on first use.
   */
  async recoverQueue(queueId: string, ttl: number): Promise<number> {
    try {
      return await this.commandClient.bqRecover(
        KEYS.inflight(queueId),
        KEYS.running(queueId),
        KEYS.jobs(queueId),
        KEYS.meta(queueId),
        KEYS.wakeup,
        String(Date.now()),
        String(this.visibilityTimeoutMs),
        queueId,
        String(ttl),
        this.poolId,
      );
    } catch {
      return 0;
    }
  }

  private startTicker(): void {
    /**
     * The ONE global timer for the entire system.
     * It fires every `tickIntervalMs` and drains the wakeup ZSET,
     * re-notifying workers for any queues that are now eligible.
     * This replaces ALL per-limiter timers — the key memory optimization.
     *
     * The same tick refreshes this pool's liveness heartbeat, which other
     * processes use to decide whether to notify us.
     */
    const tick = (): void => {
      void this.commandClient
        .bqWakeupCheck(
          KEYS.wakeup,
          KEYS.pools,
          String(Date.now()),
          String(WAKEUP_BATCH_SIZE),
          String(POOL_LIVE_WINDOW_MS),
        )
        .catch(() => {
          /* best-effort, will retry next tick */
        });

      void this.heartbeat();
    };

    this.tickerHandle = setInterval(tick, this.tickIntervalMs);
    // unref() allows Node.js to exit if this is the only remaining handle
    this.tickerHandle.unref();
  }

  /** Refresh this pool's liveness heartbeat in Redis. */
  private async heartbeat(): Promise<void> {
    try {
      await this.commandClient.bqPoolHeartbeat(
        KEYS.pools,
        this.poolId,
        String(POOL_LIVE_WINDOW_MS),
      );
    } catch {
      /* best-effort */
    }
  }

  /**
   * Periodically scans `pendingJobs` and rejects any entry that has been
   * waiting longer than `pendingJobTimeoutMs`. This catches jobs whose Redis
   * BRPOP notification was lost (e.g. Redis restart, network blip) and would
   * otherwise hold their closures in memory forever.
   */
  private startSweeper(): void {
    const sweep = (): void => {
      const cutoff = Date.now() - this.pendingJobTimeoutMs;
      for (const [jobId, pending] of this.pendingJobs) {
        if (pending.enqueuedAt < cutoff) {
          this.pendingJobs.delete(jobId);
          pending.reject(new OrphanedJobError(jobId));
          this.dispatchFailed(pending.queueId, jobId, new OrphanedJobError(jobId));
        }
      }
    };

    this.sweeperHandle = setInterval(sweep, SWEEPER_INTERVAL_MS);
    this.sweeperHandle.unref();
  }
}
