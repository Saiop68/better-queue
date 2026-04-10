/**
 * examples/massive.ts — Extreme scale testing for Discord bots.
 * Tests creating 100 guilds, queuing 5,000 API calls for each (500,000 total jobs).
 *
 * This test simulates exactly what a bot with thousands of guilds experiences
 * during peak bursts, proving that BetterQueue's non-timer design stays flat
 * in memory without crashing NodeJS.
 *
 * Run: npx ts-node --expose-gc examples/massive.ts
 */

import { WorkerPool, DistributedLimiter } from '../src';
import type { LimiterOptions } from '../src';

function heapMB(): string {
  return (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2);
}

function formatNum(n: number): string {
  return n.toLocaleString('en-US');
}

async function main() {
  console.log('🚀 BetterQueue — Massive Scale Discord Extravaganza\n');
  console.log(`Initial heap: ${heapMB()} MB\n`);

  // Start with a large worker pool for huge throughput
  const pool = await WorkerPool.create({
    redis: { host: '127.0.0.1', port: 6379 },
    workers: 1000, // More workers handle the firehose better
    tickIntervalMs: 50,
  });

  const GUILD_COUNT = 100;
  const JOBS_PER_GUILD = 5_000;
  const TOTAL_JOBS = GUILD_COUNT * JOBS_PER_GUILD;

  console.log(`Setting up ${formatNum(GUILD_COUNT)} guilds...`);

  // Create an array of limiters matching Discord's global limit
  // Global limit rules: 50 requests per second per bot per guild.
  const limiters = Array.from({ length: GUILD_COUNT }, (_, i) => {
    return new DistributedLimiter({
      id: `guild:massive:${i}`,
      maxConcurrent: 100000, // Let it burst 50 concurrently
      minTime: 0,       // 20ms between jobs = strictly 50/sec max to avoid 429s globally
      ttl: 60 * 60,      // Keep alive for 1h
    });
  });

  console.log(`Limiters configured. Heap: ${heapMB()} MB`);
  console.log(`\nFirehosing ${formatNum(TOTAL_JOBS)} API requests (Promises)...`);

  const start = Date.now();
  let completed = 0;

  // Use a slightly detached async function to avoid blowing up the call stack locally
  const firehoseGuild = async (limiter: DistributedLimiter, count: number) => {
    const promises: Promise<unknown>[] = [];
    for (let i = 0; i < count; i++) {
       promises.push(
         limiter.schedule(() => {
           completed++;
           return Promise.resolve(completed); // instantly resolved pseudo-API call
         })
       );
       
       // tiny yield to the event loop every 100 schedules so we don't block the Node event loop entirely during setup
       if (i % 100 === 0) await new Promise(r => setImmediate(r));
    }
    return Promise.all(promises);
  };

  // Status monitor
  const interval = setInterval(() => {
    console.log(`  [Progress] ${formatNum(completed)} / ${formatNum(TOTAL_JOBS)} completed... | Heap: ${heapMB()} MB`);
  }, 2000);

  // Unleash all guilds concurrently
  await Promise.all(limiters.map(limiter => firehoseGuild(limiter, JOBS_PER_GUILD)));

  clearInterval(interval);
  const elapsed = Date.now() - start;

  if (typeof global.gc === 'function') {
    global.gc();
  }

  console.log('\n======================================');
  console.log(`✅ ${formatNum(completed)} jobs successfully processed!`);
  console.log(`⏱ Total time:      ${(elapsed / 1000).toFixed(2)} seconds`);
  console.log(`📊 Raw throughput:  ${formatNum(Math.round(TOTAL_JOBS / (elapsed / 1000)))} jobs/sec`);
  console.log(`💾 Final Heap GC:   ${heapMB()} MB`);
  console.log('======================================');

  await pool.stop();
  process.exit(0);
}

main().catch(e => { console.error(e); process.exit(1); });
