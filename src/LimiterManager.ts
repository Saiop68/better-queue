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

import { LRUCache } from 'lru-cache';
import { DistributedLimiter } from './DistributedLimiter';
import { WorkerPool } from './WorkerPool';
import type { LimiterOptions } from './types';

// ── Feature config type ────────────────────────────────────────────────────

/**
 * Rate limit configuration for a specific feature (e.g. 'guild-member-add').
 * Mirrors Bottleneck.ConstructorOptions for compatibility.
 */
export interface FeatureConfig {
  /** Max concurrent jobs executing at once (per guild queue). */
  maxConcurrent?: number;
  /** Token bucket size — max burst. -1 or undefined = unlimited. */
  reservoir?: number;
  /** Tokens added on each refresh cycle. */
  reservoirRefreshAmount?: number;
  /** How often the reservoir refills (ms). 0 = disabled. */
  reservoirRefreshInterval?: number;
  /**
   * How often the reservoir is INCREMENTED (ms). 0 = disabled.
   *
   * Unlike `reservoirRefreshAmount` (which resets the bucket to a fixed
   * value), this ADDS `reservoirIncreaseAmount` tokens on every interval.
   */
  reservoirIncreaseInterval?: number;
  /** Tokens ADDED to the reservoir on each increase interval. */
  reservoirIncreaseAmount?: number;
  /** Maximum value the reservoir may reach when increasing. 0 = unbounded. */
  reservoirIncreaseMaximum?: number;
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
export interface ScheduleJobOptions {
  /** Optional job identifier (for deduplication / logging). */
  id?: string;
  /** Lower number = higher priority. Default: 5. */
  priority?: number;
  /** Schedule this job to start no earlier than this Unix-ms timestamp. */
  scheduleAt?: number;
}

// ── Defaults ───────────────────────────────────────────────────────────────

const DEFAULT_CONFIG = Object.freeze<Required<Omit<FeatureConfig, 'trackDoneStatus'>>>({
  maxConcurrent:            10,
  reservoir:                -1,   // -1 = unlimited (no token bucket)
  reservoirRefreshAmount:   10,
  reservoirRefreshInterval: 1_000,
  reservoirIncreaseInterval: 0,  // 0 = disabled (token-bucket "add" mode)
  reservoirIncreaseAmount:  0,
  reservoirIncreaseMaximum: 0,   // 0 = unbounded
  minTime:                  0,
  timeout:                  30_000,
  ttl:                      600,     // 10 minutes — idle queues auto-expire in Redis
  maxRetries:               3,
});

// ── LimiterWrapper ─────────────────────────────────────────────────────────

/**
 * Thin wrapper around DistributedLimiter that exposes the same `.schedule()`
 * overload Bottleneck uses, so all existing call-sites work without changes.
 */
export class LimiterWrapper {
  private readonly _limiter: DistributedLimiter;
  private readonly _timeout: number;

  constructor(limiter: DistributedLimiter, timeout: number) {
    this._limiter = limiter;
    this._timeout = timeout;
  }

  // ── schedule overloads (Bottleneck-compatible) ───────────────────────────

  /** `limiter.schedule(fn)` */
  schedule<T>(fn: () => Promise<T>): Promise<T>;
  /** `limiter.schedule({ id, priority }, fn)` */
  schedule<T>(options: ScheduleJobOptions, fn: () => Promise<T>): Promise<T>;

  schedule<T>(
    optionsOrFn: ScheduleJobOptions | (() => Promise<T>),
    fn?: () => Promise<T>,
  ): Promise<T> {
    let task: () => Promise<T>;
    let jobOpts: { priority?: number; scheduleAt?: number } | undefined;

    if (typeof optionsOrFn === 'function') {
      task = optionsOrFn;
    } else {
      if (!fn) throw new TypeError('BetterQueue: schedule() called with options but no function');
      task   = fn;
      jobOpts = {
        ...(optionsOrFn.priority   !== undefined && { priority:   optionsOrFn.priority }),
        ...(optionsOrFn.scheduleAt !== undefined && { scheduleAt: optionsOrFn.scheduleAt }),
      };
    }

    // Wrap task with a deadline if configured
    const timeoutMs = this._timeout;
    const timedTask: () => Promise<T> =
      timeoutMs > 0
        ? () =>
            Promise.race<T>([
              task(),
              new Promise<T>((_, reject) => {
                const handle = setTimeout(
                  () => reject(new Error(`BetterQueue: job timed out after ${timeoutMs}ms`)),
                  timeoutMs,
                );
                // Allow Node to exit even if the timeout fires
                if (handle.unref) handle.unref();
              }),
            ])
        : task;

    return this._limiter.schedule(timedTask, jobOpts);
  }

  // ── Bottleneck-compatible observability ──────────────────────────────────

  /**
   * Returns job counts in Bottleneck's format.
   * Useful for code that calls `limiter.counts()` for health checks.
   */
  async counts(): Promise<{
    RECEIVED: number;
    QUEUED:   number;
    RUNNING:  number;
    EXECUTING: number;
    DONE:     number;
  }> {
    const stats = await this._limiter.getStats();
    return {
      RECEIVED:  0,               // not tracked (no in-process queue)
      QUEUED:    stats.queueSize,
      RUNNING:   stats.runningCount,
      EXECUTING: stats.runningCount,
      DONE:      0,               // not tracked (Redis cleans up after completion)
    };
  }

  /** Direct access to the underlying DistributedLimiter if needed. */
  get inner(): DistributedLimiter {
    return this._limiter;
  }

  /** Wait until this guild's queue is fully drained (all jobs done). */
  drain(): Promise<void> {
    return this._limiter.drain();
  }

  /** Stop accepting new jobs for this limiter. */
  stop(): void {
    this._limiter.stop();
  }
}

// ── LimiterManager ─────────────────────────────────────────────────────────

/**
 * LimiterManager — singleton that owns all guild×feature rate limiters.
 *
 * Replaces the original Bottleneck-based version with BetterQueue:
 *   • Zero per-limiter timers (one global ticker for the whole process)
 *   • All queue state in Redis — limiters are tiny JS objects (~50 bytes)
 *   • Handles 100k+ concurrent guild queues without memory growth
 *   • Limiters are created lazily and cached in an LRU
 */
export class LimiterManager {
  private static _instance: LimiterManager;

  /**
   * Guild×feature limiter cache.
   * Key: `${guildId}::${feature}`
   * Large max to avoid evicting active guild limiters.
   */
  private readonly _limiterCache: LRUCache<string, LimiterWrapper>;

  /**
   * Per-feature default configs registered via `registerFeatureConfig()`.
   */
  private readonly _featureConfigs: LRUCache<string, FeatureConfig>;

  private constructor() {
    this._limiterCache = new LRUCache<string, LimiterWrapper>({
      max: 10_000,
      // 10 min TTL — well beyond any in-flight Discord API call
      ttl: 1_000 * 60 * 10,
      ttlAutopurge: true,
      // CRITICAL: call stop() on every eviction (TTL expiry, LRU displacement, or
      // manual delete). This ensures removeAllListeners() always fires, preventing
      // the silent event-listener leak on LRU cache churn.
      dispose: (wrapper) => {
        wrapper.stop();
      },
    });

    this._featureConfigs = new LRUCache<string, FeatureConfig>({
      max: 200,
    });
  }

  static getInstance(): LimiterManager {
    if (!LimiterManager._instance) {
      LimiterManager._instance = new LimiterManager();
    }
    return LimiterManager._instance;
  }

  // ── Public API (matches original LimiterManager exactly) ─────────────────

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
  registerFeatureConfig(feature: string, config: Partial<FeatureConfig>): void {
    this._featureConfigs.set(feature, { ...DEFAULT_CONFIG, ...config });
  }

  /**
   * Get (or lazily create) a per-guild, per-feature rate limiter.
   *
   * @example
   * const limiter = limiterManager.getLimiter(guildId, 'guild-member-add');
   * await limiter.schedule({ id: `${guildId}-${memberId}` }, () => doWork());
   */
  getLimiter(guildId: string, feature: string): LimiterWrapper {
    const key = `${guildId}::${feature}`;
    const cached = this._limiterCache.get(key);
    if (cached) return cached;

    const cfg: FeatureConfig = this._featureConfigs.get(feature) ?? DEFAULT_CONFIG;

    // Map FeatureConfig → LimiterOptions
    // Reservoir: Bottleneck uses positive int (10 = 10 tokens), we use -1 for "none"
    const reservoirValue =
      cfg.reservoir !== undefined && cfg.reservoir > 0 ? cfg.reservoir : -1;

    const opts: LimiterOptions = {
      id:                       `bq:${feature}:${guildId}`,
      maxConcurrent:            cfg.maxConcurrent            ?? DEFAULT_CONFIG.maxConcurrent,
      minTime:                  cfg.minTime                   ?? DEFAULT_CONFIG.minTime,
      reservoir:                reservoirValue,
      reservoirRefreshAmount:   cfg.reservoirRefreshAmount    ?? DEFAULT_CONFIG.reservoirRefreshAmount,
      reservoirRefreshInterval: cfg.reservoirRefreshInterval  ?? DEFAULT_CONFIG.reservoirRefreshInterval,
      reservoirIncreaseInterval: cfg.reservoirIncreaseInterval ?? DEFAULT_CONFIG.reservoirIncreaseInterval,
      reservoirIncreaseAmount:  cfg.reservoirIncreaseAmount   ?? DEFAULT_CONFIG.reservoirIncreaseAmount,
      reservoirIncreaseMaximum: cfg.reservoirIncreaseMaximum  ?? DEFAULT_CONFIG.reservoirIncreaseMaximum,
      ttl:                      cfg.ttl                       ?? DEFAULT_CONFIG.ttl,
      maxRetries:               cfg.maxRetries                ?? DEFAULT_CONFIG.maxRetries,
      retryOnFailure:           (cfg.maxRetries ?? DEFAULT_CONFIG.maxRetries) > 0,
    };

    const limiter = new DistributedLimiter(opts);
    const wrapper = new LimiterWrapper(limiter, cfg.timeout ?? DEFAULT_CONFIG.timeout);

    this._limiterCache.set(key, wrapper);

    return wrapper;
  }

  /**
   * Remove a single guild+feature limiter from the cache.
   * The limiter is stopped immediately, freeing its event listeners.
   *
   * Previously a no-op because the method did not exist — NightmodeQueue was
   * already trying to call this. Now it works.
   *
   * @example
   * // In guildDelete or per-feature cleanup:
   * limiterManager.removeLimiter(guildId, 'nightmode-scheduler');
   */
  removeLimiter(guildId: string, feature: string): void {
    const key = `${guildId}::${feature}`;
    // LRU dispose callback calls wrapper.stop() automatically on delete
    this._limiterCache.delete(key);
  }

  /**
   * Remove ALL limiters for a specific guild and stop them immediately.
   * Call this from the `guildDelete` Discord event to free memory for
   * guilds that have left — their limiters would otherwise sit in the LRU
   * for up to 10 minutes.
   *
   * @example
   * client.on('guildDelete', (guild) => {
   *   limiterManager.removeGuildLimiters(guild.id);
   * });
   */
  removeGuildLimiters(guildId: string): void {
    const prefix = `${guildId}::`;
    // LRUCache doesn't support prefix queries, so we collect keys to delete.
    // This is O(n) but only fires on guildDelete — an infrequent event.
    const toDelete: string[] = [];
    for (const key of this._limiterCache.keys()) {
      if (key.startsWith(prefix)) toDelete.push(key);
    }
    for (const key of toDelete) {
      // dispose callback fires stop() on each deletion
      this._limiterCache.delete(key);
    }
  }

  /**
   * Stop and remove all cached limiters.
   * Call on graceful shutdown.
   */
  async clearAll(): Promise<void> {
    // dispose callback fires stop() on every entry automatically
    this._limiterCache.clear();
    this._featureConfigs.clear();
  }

  /**
   * Cache utilization stats — matches original LimiterManager.getStats() format.
   * Now also exposes `pendingJobsSize` — the #1 indicator of a job-queue backlog.
   * If this number climbs continuously, jobs are queuing faster than workers execute.
   */
  getStats() {
    const limiterMax   = this._limiterCache.max  ?? 10_000;
    const featureMax   = this._featureConfigs.max ?? 200;
    const limiterUsage = (this._limiterCache.size   / limiterMax)  * 100;
    const featureUsage = (this._featureConfigs.size / featureMax)  * 100;

    let pendingJobsSize = 0;
    try {
      pendingJobsSize = WorkerPool.getInstance().pendingJobs.size;
    } catch {
      // WorkerPool not yet initialized — safe to ignore
    }

    if (limiterUsage > 80) {
      console.warn(
        `[BetterQueue] Limiter cache usage is high: ${limiterUsage.toFixed(1)}%` +
        ` (${this._limiterCache.size}/${limiterMax})`,
      );
    }

    if (pendingJobsSize > 500) {
      console.warn(
        `[BetterQueue] pendingJobs backlog is high: ${pendingJobsSize} entries. ` +
        `Jobs may be queuing faster than workers can drain them.`,
      );
    }

    return {
      limiterCache: {
        size:  this._limiterCache.size,
        max:   limiterMax,
        usage: `${limiterUsage.toFixed(1)}%`,
        ttl:   this._limiterCache.ttl,
      },
      featureConfigs: {
        size:  this._featureConfigs.size,
        max:   featureMax,
        usage: `${featureUsage.toFixed(1)}%`,
        ttl:   this._featureConfigs.ttl,
      },
      // Primary leak indicator — monitor this value over time
      pendingJobsSize,
    };
  }

  /** Health report — matches original LimiterManager.getCacheUsage() format. */
  getCacheUsage() {
    const stats = this.getStats();
    const usageNum = parseFloat(stats.limiterCache.usage);
    return {
      status:         stats.limiterCache.usage,
      limiterCount:   stats.limiterCache.size,
      maxLimiters:    stats.limiterCache.max,
      isHealthy:      usageNum < 80,
      recommendation: usageNum > 80
        ? 'Consider increasing cache size or optimizing feature usage'
        : 'Cache usage is healthy',
    };
  }

  /**
   * Drain a specific guild+feature queue — wait until all pending jobs complete.
   */
  async drainGuild(guildId: string, feature: string): Promise<void> {
    const key = `${guildId}::${feature}`;
    const wrapper = this._limiterCache.get(key);
    if (wrapper) await wrapper.drain();
  }
}

// ── Singleton export (matches original: `import limiterManager from '...'`) ─

export const limiterManager = LimiterManager.getInstance();
export default limiterManager;
