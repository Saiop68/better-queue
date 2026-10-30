/**
 * benchmark/compare.ts
 *
 * Head-to-head: Bottleneck 2.19.5 vs BetterQueue 1.0.1
 *
 * Three measurements, all on the same machine, same Redis:
 *
 *   1. MEMORY      — RSS after creating N limiters and scheduling work.
 *                    Bottleneck creates one setInterval per limiter. That is
 *                    the whole ballgame for large fleets.
 *   2. THROUGHPUT  — jobs/sec for a single limiter, and for 1000 limiters.
 *   3. TIMERS      — how many timers each library has alive.
 *
 * Run:
 *   REDIS_PORT=6399 npx ts-node benchmark/compare.ts
 */

import Bottleneck from 'bottleneck';
import { WorkerPool, DistributedLimiter } from '../src/index';

const REDIS = {
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: parseInt(process.env.REDIS_PORT || '6399', 10),
};

const LIMITERS = parseInt(process.env.LIMITERS || '1000', 10);
const JOBS_EACH = parseInt(process.env.JOBS_EACH || '50', 10);

function rssMb(): number {
  return Math.round(process.memoryUsage().rss / 1024 / 1024);
}

function fmt(n: number): string {
  return n.toLocaleString('en-US');
}

/**
 * Count live timers. Node's own timer list isn't public, so we instrument
 * setInterval/setTimeout before loading anything that creates timers.
 */
let intervalCount = 0;
const origSetInterval = global.setInterval;
const origSetTimeout = global.setTimeout;
// @ts-expect-error - patching for measurement
global.setInterval = function (...args: unknown[]) {
  intervalCount++;
  // @ts-expect-error - passing through
  return origSetInterval.apply(this, args);
};

async function benchBottleneck(): Promise<Record<string, number>> {
  const before = rssMb();
  const timersBefore = intervalCount;

  const limiters: Bottleneck[] = [];
  for (let i = 0; i < LIMITERS; i++) {
    const l = new Bottleneck({ id: `bench-bn-${i}`, maxConcurrent: 5, minTime: 0 });
    limiters.push(l);
  }

  const afterCreate = rssMb();
  const timersAfterCreate = intervalCount - timersBefore;

  const t0 = Date.now();
  await Promise.all(
    limiters.map((l, i) =>
      l.schedule({ id: `${i}` }, async () => {
        return 1;
      }),
    ),
  );
  // run a burst of jobs per limiter
  await Promise.all(
    limiters.flatMap((l, i) =>
      Array.from({ length: JOBS_EACH }, (_, j) =>
        l.schedule({ id: `${i}-${j}` }, async () => 1),
      ),
    ),
  );
  const elapsed = Date.now() - t0;

  const total = LIMITERS * (JOBS_EACH + 1);
  const afterJobs = rssMb();

  for (const l of limiters) await l.stop({ dropWaitingJobs: true });

  return {
    rssBeforeMb: before,
    rssAfterCreateMb: afterCreate,
    rssAfterJobsMb: afterJobs,
    createDeltaMb: afterCreate - before,
    jobsDeltaMb: afterJobs - afterCreate,
    timers: timersAfterCreate,
    elapsedMs: elapsed,
    throughput: Math.round(total / (elapsed / 1000)),
    totalJobs: total,
  };
}

async function benchBetterQueue(): Promise<Record<string, number>> {
  const before = rssMb();
  const timersBefore = intervalCount;

  const pool = await WorkerPool.create({
    redis: REDIS,
    workers: 16,
  });

  const afterCreate = rssMb();
  const timersAfterCreate = intervalCount - timersBefore;

  const limiters: DistributedLimiter[] = [];
  for (let i = 0; i < LIMITERS; i++) {
    limiters.push(
      new DistributedLimiter({
        id: `bench-bq-${i}`,
        maxConcurrent: 5,
        minTime: 0,
      }),
    );
  }

  const t0 = Date.now();
  await Promise.all(
    limiters.map((l, i) => l.schedule(async () => 1)),
  );
  await Promise.all(
    limiters.flatMap((l, i) =>
      Array.from({ length: JOBS_EACH }, (_, j) => l.schedule(async () => 1)),
    ),
  );
  const elapsed = Date.now() - t0;

  const total = LIMITERS * (JOBS_EACH + 1);
  const afterJobs = rssMb();

  await pool.stop();

  return {
    rssBeforeMb: before,
    rssAfterCreateMb: afterCreate,
    rssAfterJobsMb: afterJobs,
    createDeltaMb: afterCreate - before,
    jobsDeltaMb: afterJobs - afterCreate,
    timers: timersAfterCreate,
    elapsedMs: elapsed,
    throughput: Math.round(total / (elapsed / 1000)),
    totalJobs: total,
  };
}

async function main(): Promise<void> {
  console.log('');
  console.log('  BetterQueue vs Bottleneck');
  console.log(`  ${LIMITERS} limiters x ${JOBS_EACH + 1} jobs each`);
  console.log(`  Redis ${REDIS.host}:${REDIS.port}`);
  console.log('  ' + '-'.repeat(58));

  console.log('\n  running Bottleneck...');
  const bn = await benchBottleneck();
  console.log('  running BetterQueue...');
  const bq = await benchBetterQueue();

  const rows: [string, string, string][] = [
    [
      'RSS after creating limiters',
      `${bn.createDeltaMb} MB`,
      `${bq.createDeltaMb} MB`,
    ],
    [
      'RSS after running all jobs',
      `${bn.jobsDeltaMb} MB`,
      `${bq.jobsDeltaMb} MB`,
    ],
    [
      'live timers created',
      fmt(bn.timers),
      fmt(bq.timers),
    ],
    [
      'elapsed',
      `${bn.elapsedMs} ms`,
      `${bq.elapsedMs} ms`,
    ],
    [
      'throughput',
      `${fmt(bn.throughput)} jobs/s`,
      `${fmt(bq.throughput)} jobs/s`,
    ],
  ];

  console.log('\n  ' + '-'.repeat(58));
  console.log(`  ${''.padEnd(30)}${'Bottleneck'.padEnd(14)}BetterQueue`);
  console.log('  ' + '-'.repeat(58));
  for (const [label, a, b] of rows) {
    console.log(`  ${label.padEnd(30)}${a.padEnd(14)}${b}`);
  }
  console.log('  ' + '-'.repeat(58));
  console.log('');

  const memRatio =
    bn.jobsDeltaMb > 0 ? (bq.jobsDeltaMb / bn.jobsDeltaMb).toFixed(2) : 'n/a';
  console.log(`  memory ratio (BetterQueue / Bottleneck): ${memRatio}x`);
  console.log(`  timer ratio: ${bn.timers > 0 ? (bq.timers / bn.timers).toFixed(4) : 'n/a'}x`);
  console.log('');
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
