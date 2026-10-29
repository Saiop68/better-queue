/**
 * Shared test helpers.
 *
 * IMPORTANT: every test in this suite talks to the ISOLATED Redis instance on
 * port 6399. Port 6379 is production Redis and must never be touched.
 */
import Redis from 'ioredis';
import { WorkerPool } from '../src/index';

export const TEST_REDIS = {
  host: '127.0.0.1',
  port: 6399,
} as const;

/** Direct client for assertions/cleanup (not used by the library itself). */
export function createTestClient(): Redis {
  return new Redis({ ...TEST_REDIS, maxRetriesPerRequest: 3 });
}

/** Remove every BetterQueue key from the isolated test Redis. */
export async function flushTestRedis(client: Redis): Promise<void> {
  const keys = await client.keys('bq:*');
  if (keys.length > 0) await client.del(...keys);
}

/** Start the global WorkerPool (idempotent — returns the existing pool). */
export async function startPool(): Promise<WorkerPool> {
  return WorkerPool.create({
    redis: { ...TEST_REDIS },
    workers: 4,
    tickIntervalMs: 25,
    visibilityTimeoutMs: 60_000,
    pendingJobTimeoutMs: 0, // disable the orphan sweeper during tests
  });
}

/** Stop the global WorkerPool and close connections. */
export async function stopPool(): Promise<void> {
  try {
    const pool = WorkerPool.getInstance();
    await pool.stop();
  } catch {
    // pool was never started
  }
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Poll `fn` until it returns a truthy value or the timeout elapses. */
export async function waitFor<T>(
  fn: () => Promise<T | null | undefined | false>,
  timeoutMs = 10_000,
  intervalMs = 25,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const result = await fn();
    if (result) return result as T;
    if (Date.now() > deadline) {
      throw new Error(`waitFor() timed out after ${timeoutMs}ms`);
    }
    await sleep(intervalMs);
  }
}
