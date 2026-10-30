/**
 * benchmark/compare.mjs
 *
 * Head-to-head: Bottleneck 2.19.5 vs BetterQueue 1.0.1
 *
 * IMPORTANT: each library runs in its OWN process. Running both in one
 * process contaminates the second measurement — it inherits the first
 * library's heap, which makes the numbers meaningless. The driver script
 * (benchmark/run.sh) spawns one process per library.
 *
 * Usage:
 *   node --expose-gc benchmark/compare.mjs bottleneck
 *   node --expose-gc benchmark/compare.mjs betterqueue
 *
 * Env:
 *   REDIS_HOST (default 127.0.0.1)
 *   REDIS_PORT (default 6399)
 *   LIMITERS   (default 1000)
 *   JOBS_EACH  (default 50)
 */

const TARGET = process.argv[2] || 'bottleneck';

const REDIS = {
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: parseInt(process.env.REDIS_PORT || '6399', 10),
};

const LIMITERS = parseInt(process.env.LIMITERS || '1000', 10);
const JOBS_EACH = parseInt(process.env.JOBS_EACH || '50', 10);

const mb = (bytes) => Math.round(bytes / 1024 / 1024);

/** Live timers according to Node itself, not by monkey-patching. */
function liveTimers() {
  const resources = process.getActiveResourcesInfo?.() ?? [];
  return resources.filter((r) => r === 'Timeout' || r === 'Immediate').length;
}

function mem() {
  const m = process.memoryUsage();
  return { rss: mb(m.rss), heap: mb(m.heapUsed) };
}

function gc() {
  if (global.gc) {
    global.gc();
    global.gc();
  }
}

async function runBottleneck() {
  const { default: Bottleneck } = await import('bottleneck');

  const base = mem();
  const timersBefore = liveTimers();

  const limiters = [];
  for (let i = 0; i < LIMITERS; i++) {
    limiters.push(
      new Bottleneck({ id: `bench-bn-${i}`, maxConcurrent: 5, minTime: 0 }),
    );
  }
  const afterCreate = mem();

  const t0 = Date.now();
  await Promise.all(
    limiters.map((l, i) => l.schedule({ id: `${i}` }, async () => 1)),
  );
  await Promise.all(
    limiters.flatMap((l, i) =>
      Array.from({ length: JOBS_EACH }, (_, j) =>
        l.schedule({ id: `${i}-${j}` }, async () => 1),
      ),
    ),
  );
  const elapsed = Date.now() - t0;

  const afterJobs = mem();
  gc();
  const afterGc = mem();

  const total = LIMITERS * (JOBS_EACH + 1);
  const timersAfter = liveTimers() - timersBefore;

  for (const l of limiters) await l.stop({ dropWaitingJobs: true });

  return {
    base, afterCreate, afterJobs, afterGc,
    timers: timersAfter,
    elapsedMs: elapsed,
    throughput: Math.round(total / (elapsed / 1000)),
    totalJobs: total,
  };
}

async function runBetterQueue() {
  const { WorkerPool, DistributedLimiter } = await import('../dist/index.mjs');

  const base = mem();
  const timersBefore = liveTimers();

  const pool = await WorkerPool.create({ redis: REDIS, workers: 16 });
  const afterCreate = mem();

  const limiters = [];
  for (let i = 0; i < LIMITERS; i++) {
    limiters.push(
      new DistributedLimiter({ id: `bench-bq-${i}`, maxConcurrent: 5, minTime: 0 }),
    );
  }

  const t0 = Date.now();
  await Promise.all(limiters.map((l) => l.schedule(async () => 1)));
  await Promise.all(
    limiters.flatMap((l) =>
      Array.from({ length: JOBS_EACH }, () => l.schedule(async () => 1)),
    ),
  );
  const elapsed = Date.now() - t0;

  const afterJobs = mem();
  gc();
  const afterGc = mem();

  const total = LIMITERS * (JOBS_EACH + 1);
  const timersAfter = liveTimers() - timersBefore;

  await pool.stop();

  return {
    base, afterCreate, afterJobs, afterGc,
    timers: timersAfter,
    elapsedMs: elapsed,
    throughput: Math.round(total / (elapsed / 1000)),
    totalJobs: total,
  };
}

const runners = { bottleneck: runBottleneck, betterqueue: runBetterQueue };
const run = runners[TARGET];
if (!run) {
  console.error(`unknown target "${TARGET}" — use bottleneck | betterqueue`);
  process.exit(2);
}

const result = await run();
console.log(JSON.stringify({ target: TARGET, node: process.version, limiters: LIMITERS, jobsEach: JOBS_EACH, ...result }));
