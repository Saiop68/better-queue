import type Redis from 'ioredis';
import type { RedisOptions } from 'ioredis';
import { EventEmitter } from 'node:events';
import { createRedisClient, createBlockingClient } from './RedisClient';
import { KEYS, DEFAULT_WORKER_COUNT, DEFAULT_TICK_INTERVAL_MS, DEFAULT_VISIBILITY_TIMEOUT_MS, BRPOP_TIMEOUT_SECONDS, MAX_DRAIN_PER_NOTIFY, WAKEUP_BATCH_SIZE } from './constants';
import { WorkerPoolNotInitializedError, JobMaxRetriesError } from './errors';
import type { WorkerPoolOptions, PendingJob, JobPayload, DequeueResult } from './types';

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

  constructor(id: number, redisOptions: RedisOptions, pool: WorkerPool) {
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
        // BRPOP blocks until a queue ID arrives or times out (1s)
        const result = await this.blockingConn.brpop(
          KEYS.notify,
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

      if (status === 'rate_limited' && nextEligibleMs > 0) {
        // Schedule a wakeup — the ticker will re-notify workers at the right time
        await this.scheduleWakeup(queueId, nextEligibleMs);
      }

      // For all non-ok statuses (empty, concurrent_full, rate_limited), stop draining
      break;
    }
  }

  private async tryDequeue(queueId: string): Promise<DequeueResult | null> {
    try {
      const raw = await this.pool.commandClient.bqDequeue(
        KEYS.jobs(queueId),
        KEYS.running(queueId),
        KEYS.inflight(queueId),
        KEYS.meta(queueId),
        KEYS.reservoir(queueId),
        KEYS.lastRun(queueId),
        KEYS.lastRefresh(queueId),
        String(Date.now()),
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
      // Job fn not found — likely from a previous process instance (crash recovery).
      // Just clean up the Redis state.
      await this.completeJob(queueId, job.id);
      return;
    }

    this.pool.emit('executing', job.id);

    try {
      const value = await pending.fn();
      // Delete from map BEFORE resolving to free memory ASAP
      this.pool.pendingJobs.delete(job.id);
      await this.completeJob(queueId, job.id);
      pending.resolve(value);
      this.pool.emit('done', job.id);
    } catch (err: unknown) {
      await this.handleJobError(queueId, job, pending, err);
    }
  }

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
      const scheduleAt = Date.now() + delayMs;
      const retryPayload: JobPayload = { ...job, retryCount: attempt };

      this.pool.emit('retry', job.id, attempt);

      // Keep fn + callbacks in map — they'll be used on retry
      try {
        await this.pool.commandClient.bqEnqueue(
          KEYS.jobs(queueId),
          KEYS.meta(queueId),
          KEYS.notify,
          JSON.stringify(retryPayload),
          String(scheduleAt),
          String(this.pool.defaultTtl),
          queueId,
        );
      } catch {
        // If re-enqueue fails, fall through to failure handling
        this.pool.pendingJobs.delete(job.id);
        const retryErr = new JobMaxRetriesError(job.id, attempt, err);
        pending.reject(retryErr);
        this.pool.emit('failed', job.id, retryErr);
        return;
      }

      await this.completeJob(queueId, job.id);
    } else {
      // Max retries exhausted
      this.pool.pendingJobs.delete(job.id);
      await this.completeJob(queueId, job.id);
      const finalErr = new JobMaxRetriesError(job.id, attempt, err);
      pending.reject(finalErr);
      this.pool.emit('failed', job.id, finalErr);
    }
  }

  private async completeJob(queueId: string, jobId: string): Promise<void> {
    try {
      await this.pool.commandClient.bqComplete(
        KEYS.running(queueId),
        KEYS.inflight(queueId),
        KEYS.jobs(queueId),
        KEYS.notify,
        jobId,
        queueId,
      );
    } catch {
      // Best-effort — not fatal
    }
  }

  private async scheduleWakeup(queueId: string, atMs: number): Promise<void> {
    try {
      // ZADD bq:global:wakeup <atMs> <queueId>
      // NX = only add if not already scheduled (don't push wakeup back further)
      await this.pool.commandClient.zadd(KEYS.wakeup, 'NX', atMs, queueId);
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
 * handles deferred wakeups (for minTime / reservoir rate limiting), replacing
 * the need for any per-limiter timers.
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

  readonly defaultTtl: number;

  private readonly workers: Worker[];
  private readonly redisOptions: RedisOptions;
  private readonly tickIntervalMs: number;
  private readonly visibilityTimeoutMs: number;
  private tickerHandle: ReturnType<typeof setInterval> | null = null;
  private stopped = false;

  private constructor(options: WorkerPoolOptions) {
    super();

    const workerCount         = options.workers          ?? DEFAULT_WORKER_COUNT;
    this.tickIntervalMs       = options.tickIntervalMs   ?? DEFAULT_TICK_INTERVAL_MS;
    this.visibilityTimeoutMs  = options.visibilityTimeoutMs ?? DEFAULT_VISIBILITY_TIMEOUT_MS;
    this.defaultTtl           = 86_400;
    this.redisOptions         = options.redis;

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

  private async start(): Promise<void> {
    for (const worker of this.workers) {
      worker.start();
    }

    this.startTicker();
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

    // Stop all workers
    await Promise.all(this.workers.map((w) => w.stop()));

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
        KEYS.notify,
        String(Date.now()),
        String(this.visibilityTimeoutMs),
        queueId,
        String(ttl),
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
     */
    const tick = (): void => {
      void this.commandClient
        .bqWakeupCheck(
          KEYS.wakeup,
          KEYS.notify,
          String(Date.now()),
          String(WAKEUP_BATCH_SIZE),
        )
        .catch(() => {
          /* best-effort, will retry next tick */
        });
    };

    this.tickerHandle = setInterval(tick, this.tickIntervalMs);
    // unref() allows Node.js to exit if this is the only remaining handle
    this.tickerHandle.unref();
  }
}
