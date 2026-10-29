import { randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';
import { WorkerPool } from './WorkerPool';
import { KEYS, DEFAULT_PRIORITY, DEFAULT_MAX_RETRIES, DEFAULT_TTL_SECONDS, POOL_LIVE_WINDOW_MS } from './constants';
import { LimiterStoppedError } from './errors';
import type {
  LimiterOptions,
  ResolvedLimiterOptions,
  ScheduleOptions,
  QueueStats,
  LimiterEvents,
} from './types';

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
export class DistributedLimiter extends EventEmitter {
  private readonly opts: ResolvedLimiterOptions;
  private readonly pool: WorkerPool;
  private stopped = false;
  private metaSynced = false;
  private recoveryDone = false;
  /** Detaches this limiter's listeners from the shared WorkerPool emitter. */
  private detachPoolListeners?: () => void;

  constructor(options: LimiterOptions) {
    super();

    this.pool = WorkerPool.getInstance();

    this.opts = {
      id:                       options.id,
      maxConcurrent:            options.maxConcurrent            ?? 1,
      minTime:                  options.minTime                   ?? 0,
      reservoir:                options.reservoir                 ?? -1,
      reservoirRefreshInterval: options.reservoirRefreshInterval  ?? 0,
      reservoirRefreshAmount:   options.reservoirRefreshAmount    ?? 0,
      reservoirIncreaseInterval: options.reservoirIncreaseInterval ?? 0,
      reservoirIncreaseAmount:  options.reservoirIncreaseAmount   ?? 0,
      reservoirIncreaseMaximum: options.reservoirIncreaseMaximum  ?? 0,
      priority:                 options.priority                  ?? DEFAULT_PRIORITY,
      ttl:                      options.ttl                       ?? DEFAULT_TTL_SECONDS,
      retryOnFailure:           options.retryOnFailure            ?? true,
      maxRetries:               options.maxRetries                ?? DEFAULT_MAX_RETRIES,
    };

    // Subscribe to the pool's job events for THIS queue only, so that
    // `limiter.on('executing', ...)` fires for this limiter's own jobs.
    //
    // The subscription is tracked per queue id inside the pool, so creating
    // thousands of limiters does not grow the pool emitter's listener count.
    this.detachPoolListeners = this.pool.subscribeQueue(this.opts.id, {
      executing: (jobId: string) => { this.emit('executing', jobId); },
      done:      (jobId: string) => { this.emit('done', jobId); },
      failed:    (jobId: string, error: unknown) => { this.emit('failed', jobId, error); },
      retry:     (jobId: string, attempt: number) => { this.emit('retry', jobId, attempt); },
    });
  }

  /** The unique queue identifier. */
  get id(): string {
    return this.opts.id;
  }

  // ── Core API ───────────────────────────────────────────────────────────────

  /**
   * Schedule an async function to run under this limiter's rate limits.
   *
   * The function is enqueued in Redis and will be executed by a WorkerPool
   * worker when rate-limit conditions are satisfied. Returns a Promise that
   * resolves/rejects with the function's result.
   *
   * Both argument orders are supported, so this reads naturally whether you
   * come from Bottleneck (`schedule(options, fn)`) or from the docs
   * (`schedule(fn, options)`):
   *
   * @param fn - The async function to execute.
   * @param options - Per-job scheduling options (priority, scheduleAt, etc.)
   * @returns A Promise that resolves with the function's return value.
   *
   * @example
   * const data = await limiter.schedule(() => fetch('/api/data').then(r => r.json()));
   * const slow  = await limiter.schedule({ scheduleAt: Date.now() + 5_000 }, fn);
   */
  async schedule<T>(
    fn: () => Promise<T>,
    options?: ScheduleOptions,
  ): Promise<T>;
  async schedule<T>(
    options: ScheduleOptions,
    fn: () => Promise<T>,
  ): Promise<T>;
  async schedule<T>(
    fnOrOptions: (() => Promise<T>) | ScheduleOptions,
    optionsOrFn?: ScheduleOptions | (() => Promise<T>),
  ): Promise<T> {
    if (this.stopped) throw new LimiterStoppedError(this.opts.id);

    // Normalise the two supported argument orders.
    let fn: () => Promise<T>;
    let options: ScheduleOptions | undefined;

    if (typeof fnOrOptions === 'function') {
      fn = fnOrOptions;
      options = typeof optionsOrFn === 'function' ? undefined : optionsOrFn;
    } else {
      if (typeof optionsOrFn !== 'function') {
        throw new TypeError('BetterQueue: schedule() called with options but no function');
      }
      fn = optionsOrFn;
      options = fnOrOptions;
    }

    // Lazy init: sync meta to Redis and run crash recovery on first use
    await this.ensureInitialized();

    const jobId      = randomUUID();
    const priority   = options?.priority   ?? this.opts.priority;
    const scheduleAt = options?.scheduleAt ?? 0; // 0 = run as soon as allowed
    const maxRetries = options?.maxRetries ?? (this.opts.retryOnFailure ? this.opts.maxRetries : 0);

    const payload = JSON.stringify({
      id:          jobId,
      priority,
      maxRetries,
      retryCount:  0,
      enqueuedAt:  Date.now(),
      owner:       this.pool.poolId,
    });

    return new Promise<T>((resolve, reject) => {
      // Store closure + callbacks — this is the ONLY in-process state per job.
      // Deleted as soon as the job completes or fails (in WorkerPool).
      this.pool.pendingJobs.set(jobId, {
        fn: fn as () => Promise<unknown>,
        resolve: resolve as (value: unknown) => void,
        reject,
        maxRetries,
        enqueuedAt: Date.now(),
        queueId: this.opts.id,
      });

      // Enqueue in Redis. The ZSET score (priority ordering) is composed
      // SERVER-side inside enqueue.lua, on top of the Redis server clock —
      // that keeps priority working while staying immune to client clock skew.
      this.pool.commandClient
        .bqEnqueue(
          KEYS.jobs(this.opts.id),
          KEYS.delayed(this.opts.id),
          KEYS.meta(this.opts.id),
          KEYS.wakeup,
          KEYS.pools,
          payload,
          String(scheduleAt),
          '0', // relative delay — not used for user-scheduled jobs
          String(priority),
          String(this.opts.ttl),
          this.opts.id,
          String(POOL_LIVE_WINDOW_MS),
        )
        .catch((err: unknown) => {
          // Redis failure — reject immediately and clean up
          this.pool.pendingJobs.delete(jobId);
          reject(err);
        });
    });
  }

  // ── Observability ──────────────────────────────────────────────────────────

  /**
   * Returns all queue statistics in a single atomic Redis read.
   *
   * Initializes the queue on first use (meta + reservoir) so that stats are
   * meaningful even before the first job is scheduled.
   */
  async getStats(): Promise<QueueStats> {
    await this.ensureInitialized();

    const raw = await this.pool.commandClient.bqStats(
      KEYS.jobs(this.opts.id),
      KEYS.running(this.opts.id),
      KEYS.reservoir(this.opts.id),
      KEYS.delayed(this.opts.id),
      KEYS.meta(this.opts.id),
      KEYS.lastRefresh(this.opts.id),
      KEYS.lastIncrease(this.opts.id),
    );

    const reservoir = raw[2] === '-1' || raw[2] === null ? null : parseInt(raw[2], 10);

    return {
      queueSize:    parseInt(raw[0] ?? '0', 10),
      runningCount: parseInt(raw[1] ?? '0', 10),
      reservoir,
    };
  }

  /** Number of jobs waiting to be executed. */
  async getQueueSize(): Promise<number> {
    return (await this.getStats()).queueSize;
  }

  /** Number of jobs currently executing. */
  async getRunningCount(): Promise<number> {
    return (await this.getStats()).runningCount;
  }

  /**
   * Current reservoir token count.
   * Returns `null` if reservoir is not configured for this limiter.
   */
  async getReservoir(): Promise<number | null> {
    return (await this.getStats()).reservoir;
  }

  // ── Queue management ───────────────────────────────────────────────────────

  /**
   * Update limiter settings. Changes are written to Redis and take effect on
   * the next dequeue attempt.
   */
  async updateSettings(patch: Partial<Omit<LimiterOptions, 'id'>>): Promise<void> {
    const meta: Record<string, string> = {};

    if (patch.maxConcurrent             !== undefined) meta['maxConcurrent']             = String(patch.maxConcurrent);
    if (patch.minTime                   !== undefined) meta['minTime']                   = String(patch.minTime);
    if (patch.reservoir                 !== undefined) meta['reservoir']                 = String(patch.reservoir);
    if (patch.reservoirRefreshInterval  !== undefined) meta['reservoirRefreshInterval']  = String(patch.reservoirRefreshInterval);
    if (patch.reservoirRefreshAmount    !== undefined) meta['reservoirRefreshAmount']    = String(patch.reservoirRefreshAmount);
    if (patch.reservoirIncreaseInterval !== undefined) meta['reservoirIncreaseInterval'] = String(patch.reservoirIncreaseInterval);
    if (patch.reservoirIncreaseAmount   !== undefined) meta['reservoirIncreaseAmount']   = String(patch.reservoirIncreaseAmount);
    if (patch.reservoirIncreaseMaximum  !== undefined) meta['reservoirIncreaseMaximum']  = String(patch.reservoirIncreaseMaximum);
    if (patch.ttl                       !== undefined) meta['ttl']                       = String(patch.ttl);

    if (Object.keys(meta).length === 0) return;

    await this.pool.commandClient.hset(KEYS.meta(this.opts.id), meta);
  }

  /**
   * Wait until all currently queued and running jobs finish.
   * Polls Redis every 100ms.
   *
   * @param timeoutMs - If provided, rejects with a `DrainTimeoutError` after
   *   this many milliseconds so `drain()` can never block forever when a job
   *   notification is lost. Defaults to no timeout.
   */
  async drain(timeoutMs?: number): Promise<void> {
    const deadline = timeoutMs !== undefined ? Date.now() + timeoutMs : Infinity;

    for (;;) {
      if (Date.now() >= deadline) {
        throw new Error(
          `drain() timed out after ${timeoutMs}ms — ` +
          `queue "${this.opts.id}" may have orphaned jobs.`,
        );
      }
      const stats = await this.getStats();
      if (stats.queueSize === 0 && stats.runningCount === 0) break;
      await new Promise<void>((r) => setTimeout(r, 100));
    }
    this.emit('drained');
  }

  /**
   * Stop accepting new jobs. Does NOT drain existing jobs.
   * Call `drain()` first if you want to wait for pending jobs.
   * Removes all event listeners to free any captured closures.
   */
  stop(): void {
    this.stopped = true;
    // Detach this limiter's listeners from the shared WorkerPool emitter so
    // they cannot accumulate over the lifetime of a long-running process.
    this.detachPoolListeners?.();
    this.removeAllListeners();
  }

  // ── Internal ───────────────────────────────────────────────────────────────

  /**
   * Lazily synchronize queue meta to Redis and run crash recovery.
   * Only runs once per lifteime of the DistributedLimiter instance.
   */
  private async ensureInitialized(): Promise<void> {
    if (this.metaSynced && this.recoveryDone) return;

    if (!this.metaSynced) {
      await this.syncMeta();
      this.metaSynced = true;
    }

    if (!this.recoveryDone) {
      await this.pool.recoverQueue(this.opts.id, this.opts.ttl);
      this.recoveryDone = true;
    }
  }

  /** Write resolved options to the Redis HASH so Lua scripts can read them. */
  private async syncMeta(): Promise<void> {
    const meta: Record<string, string> = {
      maxConcurrent:             String(this.opts.maxConcurrent),
      minTime:                   String(this.opts.minTime),
      reservoir:                 String(this.opts.reservoir),
      reservoirRefreshInterval:  String(this.opts.reservoirRefreshInterval),
      reservoirRefreshAmount:    String(this.opts.reservoirRefreshAmount),
      reservoirIncreaseInterval: String(this.opts.reservoirIncreaseInterval),
      reservoirIncreaseAmount:   String(this.opts.reservoirIncreaseAmount),
      reservoirIncreaseMaximum:  String(this.opts.reservoirIncreaseMaximum),
      ttl:                       String(this.opts.ttl),
    };

    const client = this.pool.commandClient;
    await client.hset(KEYS.meta(this.opts.id), meta);
    await client.expire(KEYS.meta(this.opts.id), this.opts.ttl);

    // Initialize reservoir token count only if not already set
    // (avoids resetting mid-use when the limiter is re-instantiated).
    // `reservoir: 0` is a valid configuration (start with an empty bucket),
    // so seed whenever the reservoir is configured — i.e. not -1.
    if (this.opts.reservoir >= 0) {
      const seeded = await client.set(
        KEYS.reservoir(this.opts.id),
        String(this.opts.reservoir),
        'EX', this.opts.ttl,
        'NX', // only set if not exists
      );

      // Seed the reservoir interval clocks at the same time, so the first
      // refresh/increase only happens a full interval after the bucket was
      // created — not immediately on the first dequeue.
      if (seeded === 'OK') {
        const now = String(Date.now());
        await client.set(KEYS.lastRefresh(this.opts.id), now, 'EX', this.opts.ttl);
        await client.set(KEYS.lastIncrease(this.opts.id), now, 'EX', this.opts.ttl);
      }
    }
  }
}

// ── Typed EventEmitter overloads ───────────────────────────────────────────
// These teach TypeScript the exact event signatures for type-safe `.on()` calls.

export interface DistributedLimiter {
  on<K extends keyof LimiterEvents>(event: K, listener: (...args: LimiterEvents[K]) => void): this;
  emit<K extends keyof LimiterEvents>(event: K, ...args: LimiterEvents[K]): boolean;
}
