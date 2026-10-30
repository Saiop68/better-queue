/**
 * BetterQueue — High-performance, memory-efficient, distributed rate limiter
 * and job queue for Node.js. Redis-backed. Zero per-limiter timers.
 *
 * @example
 * ```ts
 * import { WorkerPool, DistributedLimiter } from '@destroyer777777/betterqueue';
 *
 * const pool = await WorkerPool.create({
 *   redis: { host: 'localhost', port: 6379 },
 *   workers: 8,
 * });
 *
 * const limiter = new DistributedLimiter({
 *   id: 'tenant:123456789',
 *   maxConcurrent: 2,
 *   minTime: 500,
 *   reservoir: 10,
 *   reservoirRefreshInterval: 1_000,
 *   reservoirRefreshAmount: 5,
 * });
 *
 * const result = await limiter.schedule(() => someAsyncTask());
 * ```
 */

export { WorkerPool } from './WorkerPool';
export { LimiterManager, LimiterWrapper, limiterManager } from './LimiterManager';
export type { FeatureConfig, ScheduleJobOptions } from './LimiterManager';
export { DistributedLimiter } from './DistributedLimiter';
export { KEYS } from './constants';
export type { RedisConnection, SentinelOptions } from './RedisClient';
export { isSentinelOptions } from './RedisClient';
export {
  BetterQueueError,
  JobMaxRetriesError,
  LimiterStoppedError,
  WorkerPoolNotInitializedError,
  RedisCommandError,
} from './errors';

export type {
  LimiterOptions,
  ResolvedLimiterOptions,
  ScheduleOptions,
  WorkerPoolOptions,
  QueueStats,
  JobPayload,
  PendingJob,
  DequeueResult,
  DequeueStatus,
  LimiterEvents,
} from './types';
