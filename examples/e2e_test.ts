// Minimal end-to-end test — schedule 3 jobs through WorkerPool and verify they complete
import { WorkerPool, DistributedLimiter } from '../src';

process.on('unhandledRejection', (e) => { console.error('UNHANDLED:', e); process.exit(1); });

async function sleep(ms: number) { return new Promise(r => setTimeout(r, ms)); }

async function main() {
  console.log('Starting minimal E2E test...');

  const pool = await WorkerPool.create({
    redis: { host: '127.0.0.1', port: 6379 },
    workers: 2,
    tickIntervalMs: 50,
  });

  console.log('Pool created. pendingJobs size:', pool.pendingJobs.size);

  const limiter = new DistributedLimiter({
    id: 'e2e-test',
    maxConcurrent: 2,
    minTime: 0,
    ttl: 300,
  });

  console.log('Scheduling 3 jobs...');

  let completed = 0;

  const jobs = [
    limiter.schedule(async () => { console.log('  Job 1 running'); await sleep(50); completed++; return 'r1'; }),
    limiter.schedule(async () => { console.log('  Job 2 running'); await sleep(50); completed++; return 'r2'; }),
    limiter.schedule(async () => { console.log('  Job 3 running'); await sleep(50); completed++; return 'r3'; }),
  ];

  console.log('Jobs scheduled. Waiting for completions...');

  // Also poll pendingJobs size
  let ticks = 0;
  const monitor = setInterval(() => {
    ticks++;
    console.log(`  [tick ${ticks}] pendingJobs: ${pool.pendingJobs.size}, completed: ${completed}`);
    if (ticks > 20) {
      console.error('TIMEOUT — jobs did not complete in 10s');
      process.exit(1);
    }
  }, 500);

  const results = await Promise.all(jobs);
  clearInterval(monitor);

  console.log('Results:', results);
  console.log('Completed:', completed);

  await pool.stop();
  console.log('✅ Test passed!');
  process.exit(0);
}

main().catch(e => { console.error(e); process.exit(1); });
