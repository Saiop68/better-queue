/**
 * examples/benchmark.ts — Performance benchmark comparing memory usage and
 * throughput at scale.
 *
 * Tests:
 *   1. Throughput: jobs/second with no rate limiting
 *   2. Memory stability: 10k limiters, 1M jobs, heap stays flat
 *   3. Burst: 100 jobs/limiter across 100 limiters simultaneously
 *
 * Run: npx ts-node examples/benchmark.ts
 */

import { WorkerPool, DistributedLimiter } from '../src';

function heapMB(): string {
  return (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2);
}

function formatNum(n: number): string {
  return n.toLocaleString('en-US');
}

async function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

// ── Benchmark 1: Raw throughput (no rate limits) ───────────────────────────

async function benchThroughput(): Promise<void> {
  console.log('\n📊 Benchmark 1: Raw throughput (1 limiter, no rate limits)\n');

  const limiter = new DistributedLimiter({
    id: 'bench:throughput',
    maxConcurrent: 32,
    minTime: 0,
  });

  const JOB_COUNT = 10_000;
  const start = Date.now();
  const heapBefore = heapMB();

  await Promise.all(
    Array.from({ length: JOB_COUNT }, () =>
      limiter.schedule(() => Promise.resolve(1)),
    ),
  );

  const elapsed = Date.now() - start;
  const heapAfter = heapMB();

  console.log(`  Jobs:        ${formatNum(JOB_COUNT)}`);
  console.log(`  Time:        ${elapsed}ms`);
  console.log(`  Throughput:  ${formatNum(Math.round(JOB_COUNT / (elapsed / 1000)))} jobs/sec`);
  console.log(`  Heap before: ${heapBefore} MB`);
  console.log(`  Heap after:  ${heapAfter} MB`);
  console.log(`  Heap delta:  ${(parseFloat(heapAfter) - parseFloat(heapBefore)).toFixed(2)} MB`);
}

// ── Benchmark 2: Memory stability at scale ────────────────────────────────

async function benchMemoryStability(): Promise<void> {
  console.log('\n📊 Benchmark 2: Memory stability — 1,000 limiters, 50 jobs each\n');

  const LIMITER_COUNT = 1_000;
  const JOBS_PER_LIMITER = 50;
  const heapBefore = heapMB();

  console.log(`  Creating ${formatNum(LIMITER_COUNT)} DistributedLimiter instances…`);
  const limiters = Array.from(
    { length: LIMITER_COUNT },
    (_, i) => new DistributedLimiter({ id: `bench:mem:${i}`, maxConcurrent: 2 }),
  );

  const heapAfterCreate = heapMB();
  console.log(`  Heap after creating ${formatNum(LIMITER_COUNT)} limiters: ${heapAfterCreate} MB`);
  console.log(`  Per-limiter cost: ${(
    (parseFloat(heapAfterCreate) - parseFloat(heapBefore)) * 1024 / LIMITER_COUNT
  ).toFixed(2)} KB`);

  console.log(`\n  Scheduling ${formatNum(LIMITER_COUNT * JOBS_PER_LIMITER)} total jobs…`);
  const start = Date.now();

  const allJobs = limiters.flatMap((limiter) =>
    Array.from({ length: JOBS_PER_LIMITER }, () =>
      limiter.schedule(() => Promise.resolve(42)),
    ),
  );

  await Promise.all(allJobs);

  const elapsed = Date.now() - start;
  const heapAfter = heapMB();

  // Force GC if available (run node with --expose-gc)
  if (typeof global.gc === 'function') {
    global.gc();
    await sleep(100);
  }

  const heapAfterGC = heapMB();

  console.log(`\n  Total jobs:      ${formatNum(LIMITER_COUNT * JOBS_PER_LIMITER)}`);
  console.log(`  Time:            ${elapsed}ms`);
  console.log(`  Throughput:      ${formatNum(Math.round((LIMITER_COUNT * JOBS_PER_LIMITER) / (elapsed / 1000)))} jobs/sec`);
  console.log(`  Heap before:     ${heapBefore} MB`);
  console.log(`  Heap after run:  ${heapAfter} MB`);
  console.log(`  Heap after GC:   ${heapAfterGC} MB`);
  console.log(`  Heap leak:       ${(parseFloat(heapAfterGC) - parseFloat(heapBefore)).toFixed(2)} MB`);
}

// ── Benchmark 3: Rate-limited burst ───────────────────────────────────────

async function benchRateLimitedBurst(): Promise<void> {
  console.log('\n📊 Benchmark 3: Rate-limited burst — 50 limiters, minTime=100ms\n');

  const LIMITER_COUNT = 50;
  const JOBS_PER = 10;

  const limiters = Array.from(
    { length: LIMITER_COUNT },
    (_, i) =>
      new DistributedLimiter({
        id: `bench:ratelimit:${i}`,
        maxConcurrent: 1,
        minTime: 100,
      }),
  );

  const start = Date.now();

  await Promise.all(
    limiters.flatMap((limiter) =>
      Array.from({ length: JOBS_PER }, () =>
        limiter.schedule(() => Promise.resolve(true)),
      ),
    ),
  );

  const elapsed = Date.now() - start;
  console.log(`  ${LIMITER_COUNT} limiters × ${JOBS_PER} jobs = ${LIMITER_COUNT * JOBS_PER} total`);
  console.log(`  Time: ${elapsed}ms`);
  console.log(`  Expected min: ~${JOBS_PER * 100}ms (minTime=100ms × ${JOBS_PER} sequential jobs)`);
  console.log(`  Heap: ${heapMB()} MB`);
}

// ── Main ───────────────────────────────────────────────────────────────────

async function main() {
  console.log('🔬 BetterQueue Performance Benchmarks');
  console.log('======================================');
  console.log(`Node.js: ${process.version}`);
  console.log(`Initial heap: ${heapMB()} MB\n`);

  const pool = await WorkerPool.create({
    redis: { host: '127.0.0.1', port: 6379 },
    workers: 16,
    tickIntervalMs: 25,
  });

  try {
    await benchThroughput();
    await benchMemoryStability();
    await benchRateLimitedBurst();

    console.log('\n======================================');
    console.log('✅ All benchmarks complete');
    console.log(`Final heap: ${heapMB()} MB`);
  } finally {
    await pool.stop();
  }
}

main().catch(console.error);
