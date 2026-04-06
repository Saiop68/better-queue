/**
 * examples/basic.ts — Basic usage of BetterQueue.
 *
 * Run: npx ts-node examples/basic.ts
 * Requires: Redis running on localhost:6379
 */

import { WorkerPool, DistributedLimiter } from '../src';

async function simulateApiCall(label: string, duration = 100): Promise<string> {
  return new Promise((resolve) =>
    setTimeout(() => {
      console.log(`  ✓ [${new Date().toISOString()}] Completed: ${label}`);
      resolve(`result:${label}`);
    }, duration),
  );
}

async function main() {
  console.log('🚀 BetterQueue — Basic Example\n');

  // ── 1. Start the global worker pool ────────────────────────────────────────
  const pool = await WorkerPool.create({
    redis: { host: '127.0.0.1', port: 6379 },
    workers: 4,
    tickIntervalMs: 50,
  });

  console.log('✅ WorkerPool started with 4 workers\n');

  // ── 2. Simple rate-limited queue ───────────────────────────────────────────
  console.log('--- Example 1: maxConcurrent=1, minTime=200ms ---');

  const limiter = new DistributedLimiter({
    id: 'example:basic',
    maxConcurrent: 1,
    minTime: 200, // minimum 200ms between job starts
  });

  const start = Date.now();

  const results = await Promise.all([
    limiter.schedule(() => simulateApiCall('job-1', 50)),
    limiter.schedule(() => simulateApiCall('job-2', 50)),
    limiter.schedule(() => simulateApiCall('job-3', 50)),
  ]);

  console.log(`\nAll 3 jobs done in ${Date.now() - start}ms`);
  console.log('Results:', results);
  console.log('(Expected: ~600ms total due to minTime=200ms)\n');

  // ── 3. Token bucket (reservoir) ────────────────────────────────────────────
  console.log('--- Example 2: reservoir=3, refills 2 tokens/second ---');

  const bucketLimiter = new DistributedLimiter({
    id: 'example:reservoir',
    maxConcurrent: 10,
    reservoir: 3,
    reservoirRefreshInterval: 1_000,
    reservoirRefreshAmount: 2,
  });

  const stats = await bucketLimiter.getStats();
  console.log('Initial stats:', stats);

  const bucketStart = Date.now();
  const bucketJobs = Array.from({ length: 6 }, (_, i) =>
    bucketLimiter.schedule(() => simulateApiCall(`bucket-job-${i + 1}`, 10)),
  );

  await Promise.all(bucketJobs);
  console.log(`\n6 jobs with reservoir=3 (+2/s refill) done in ${Date.now() - bucketStart}ms\n`);

  // ── 4. Priority queue ──────────────────────────────────────────────────────
  console.log('--- Example 3: Priority scheduling ---');

  const priorityLimiter = new DistributedLimiter({
    id: 'example:priority',
    maxConcurrent: 1,
  });

  // Schedule lower-priority jobs first, then a high-priority job
  const p1 = priorityLimiter.schedule(() => simulateApiCall('low-priority-1', 10), {
    priority: 8,
  });
  const p2 = priorityLimiter.schedule(() => simulateApiCall('low-priority-2', 10), {
    priority: 8,
  });
  const p3 = priorityLimiter.schedule(() => simulateApiCall('HIGH-PRIORITY', 10), {
    priority: 1, // will jump the queue
  });

  await Promise.all([p1, p2, p3]);

  // ── 5. Stats ───────────────────────────────────────────────────────────────
  console.log('\n--- Stats ---');
  const finalStats = await limiter.getStats();
  console.log('Queue stats:', finalStats);

  // ── Cleanup ────────────────────────────────────────────────────────────────
  await pool.stop();
  console.log('\n✅ Done. Pool stopped.');
}

main().catch(console.error);
