/**
 * Cross-process regression tests.
 *
 * The bug this file guards against: when a shared queue's concurrency slot
 * was held by a job owned by ANOTHER process, the waiting job was never
 * re-notified. `dequeue.lua` returned `concurrent_full` with
 * `nextEligibleMs = 0` ("no idea when to retry") and `WorkerPool.drainQueue`
 * did not schedule a wakeup for that status — so the job sat in Redis until
 * an unrelated notification happened to poke the queue.
 *
 * These tests spawn REAL second Node processes so the two WorkerPools
 * genuinely compete for the same Redis-backed concurrency slots.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { spawn, type ChildProcess } from 'node:child_process';
import path from 'node:path';
import type Redis from 'ioredis';
import { DistributedLimiter } from '../src/index';
import {
  createTestClient,
  flushTestRedis,
  sleep,
  waitFor,
  startPool,
  stopPool,
  TEST_REDIS,
} from './helpers';

let redis: Redis;
let seq = 0;

function queueId(name: string): string {
  seq += 1;
  return `xproc:${name}:${seq}:${process.pid}`;
}

/** Absolute path to the child-process fixture. */
const CHILD_FIXTURE = path.join(__dirname, 'fixtures', 'child-process.ts');

interface ChildResult {
  role: string;
  ok: boolean;
  error?: string;
  poolId?: string;
  result?: string;
  scheduledAt?: number;
  startedAt?: number;
  finishedAt?: number;
}

/**
 * Spawn the child fixture as a real Node process and resolve with the JSON
 * line it prints on stdout.
 */
function runChild(
  role: 'a' | 'b',
  queue: string,
  holdMs: number,
  delayMs: number,
): { child: ChildProcess; done: Promise<ChildResult> } {
  const child = spawn(
    process.execPath,
    [
      '-r',
      'ts-node/register/transpile-only',
      CHILD_FIXTURE,
      role,
      queue,
      String(holdMs),
      String(delayMs),
      String(TEST_REDIS.port),
    ],
    {
      cwd: path.join(__dirname, '..'),
      env: { ...process.env, TS_NODE_TRANSPILE_ONLY: 'true' },
      stdio: ['ignore', 'pipe', 'pipe'],
    },
  );

  let stdout = '';
  let stderr = '';
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', (chunk: string) => { stdout += chunk; });
  child.stderr.on('data', (chunk: string) => { stderr += chunk; });

  const done = new Promise<ChildResult>((resolve, reject) => {
    const timer = setTimeout(() => {
      child.kill('SIGKILL');
      reject(new Error(`child "${role}" timed out. stdout=${stdout} stderr=${stderr}`));
    }, 30_000);

    child.on('error', (err) => { clearTimeout(timer); reject(err); });
    child.on('close', () => {
      clearTimeout(timer);
      const line = stdout
        .split('\n')
        .map((l) => l.trim())
        .filter((l) => l.startsWith('{'))
        .pop();

      if (!line) {
        reject(new Error(`child "${role}" produced no result. stdout=${stdout} stderr=${stderr}`));
        return;
      }
      try {
        resolve(JSON.parse(line) as ChildResult);
      } catch (err) {
        reject(new Error(`child "${role}" produced invalid JSON: ${line} (${String(err)})`));
      }
    });
  });

  return { child, done };
}

beforeAll(async () => {
  redis = createTestClient();
});

afterAll(async () => {
  await flushTestRedis(redis);
  await redis.quit();
});

// ─────────────────────────────────────────────────────────────────────────────

describe('cross-process concurrency', () => {
  it('runs a job from process B once process A frees the concurrency slot', async () => {
    const queue = queueId('stall');
    const HOLD_MS = 2_000;
    const DELAY_MS = 900;

    await flushTestRedis(redis);

    // Process A grabs the single concurrency slot and holds it for 2s.
    const a = runChild('a', queue, HOLD_MS, DELAY_MS);

    // Give A a moment to actually start the holder job.
    await waitFor(async () => (await redis.scard(`bq:${queue}:running`)) === 1, 10_000);

    // Process B schedules a priority-1 job while the slot is held.
    const b = runChild('b', queue, HOLD_MS, DELAY_MS);

    const [resultA, resultB] = await Promise.all([a.done, b.done]);

    expect(resultA.ok).toBe(true);
    expect(resultB.ok).toBe(true);
    expect(resultB.result).toBe('RAN');

    // The two jobs must NOT overlap — maxConcurrent is global across processes.
    expect(resultB.startedAt!).toBeGreaterThanOrEqual(resultA.finishedAt!);

    // …and B must not have started before A even began.
    expect(resultB.startedAt!).toBeGreaterThanOrEqual(resultA.startedAt!);

    // The regression: B's job used to sit in Redis for ~10s (or forever).
    // It must start within a short, bounded time after the slot is freed.
    const latencyAfterSlotFreed = resultB.startedAt! - resultA.finishedAt!;
    expect(latencyAfterSlotFreed).toBeLessThan(1_000);

    // And the whole thing must be done well before the old 10s stall.
    expect(resultB.startedAt! - resultA.startedAt!).toBeLessThan(3_000);
  }, 40_000);

  it('never runs more jobs than maxConcurrent allows across processes', async () => {
    const queue = queueId('strict-concurrency');
    const HOLD_MS = 1_500;
    const DELAY_MS = 200;

    await flushTestRedis(redis);

    // Process A holds the only slot.
    const a = runChild('a', queue, HOLD_MS, DELAY_MS);
    await waitFor(async () => (await redis.scard(`bq:${queue}:running`)) === 1, 10_000);

    // Process B tries to run a job at the same time.
    const b = runChild('b', queue, HOLD_MS, DELAY_MS);

    // Watch the running SET for the whole window — it must never hold more
    // than one job, even though two processes are competing for it.
    let peak = 0;
    const watcher = setInterval(() => {
      void redis
        .scard(`bq:${queue}:running`)
        .then((n) => { if (n > peak) peak = n; })
        .catch(() => { /* ignore */ });
    }, 10);

    await Promise.all([a.done, b.done]);
    clearInterval(watcher);

    expect(peak).toBeLessThanOrEqual(1);
  }, 40_000);

  it('keeps the wakeup ZSET bounded while a slot stays held by another process', async () => {
    const queue = queueId('wakeup-bounded');
    const HOLD_MS = 3_000;
    const DELAY_MS = 300;

    await flushTestRedis(redis);

    const a = runChild('a', queue, HOLD_MS, DELAY_MS);
    await waitFor(async () => (await redis.scard(`bq:${queue}:running`)) === 1, 10_000);

    const b = runChild('b', queue, HOLD_MS, DELAY_MS);

    // Sample the wakeup ZSET while the slot is held and B is polling.
    let maxWakeup = 0;
    const sampler = setInterval(() => {
      void redis
        .zcard('bq:global:wakeup')
        .then((n) => { if (n > maxWakeup) maxWakeup = n; })
        .catch(() => { /* ignore */ });
    }, 50);

    await Promise.all([a.done, b.done]);
    clearInterval(sampler);

    // One pending wakeup per (pool, queue) pair — never more.
    const members = await redis.zrange('bq:global:wakeup', 0, -1);
    const forQueue = members.filter((m) => m.endsWith(`|${queue}`));
    expect(forQueue.length).toBeLessThanOrEqual(2); // one per participating pool
    expect(maxWakeup).toBeLessThanOrEqual(2);

    // The ZSET must not have grown without bound.
    expect(members.length).toBeLessThan(20);
  }, 40_000);

  it('does not accumulate duplicate wakeups for the same queue', async () => {
    const queue = queueId('wakeup-dedup');

    await flushTestRedis(redis);

    const workerPool = await startPool();

    try {
      // `minTime` makes every dequeue attempt come back `rate_limited`,
      // which schedules a wakeup for this queue — over and over, from
      // several jobs at once. The ZSET must still hold a SINGLE entry.
      const limiter = new DistributedLimiter({ id: queue, maxConcurrent: 5, minTime: 150 });

      const jobs = Array.from({ length: 20 }, () => limiter.schedule(async () => 1));

      // Sample the wakeup ZSET while the jobs are pacing themselves.
      let maxWakeup = 0;
      const sampler = setInterval(() => {
        void redis
          .zcard('bq:global:wakeup')
          .then((n) => { if (n > maxWakeup) maxWakeup = n; })
          .catch(() => { /* ignore */ });
      }, 25);

      await Promise.all(jobs);
      clearInterval(sampler);

      const members = await redis.zrange('bq:global:wakeup', 0, -1);
      const forQueue = members.filter((m) => m.endsWith(`|${queue}`));

      // Exactly one wakeup per (pool, queue) — the "keep the earliest" logic
      // must never fan out into duplicate entries.
      expect(forQueue.length).toBeLessThanOrEqual(1);
      expect(maxWakeup).toBeLessThanOrEqual(1);
    } finally {
      await stopPool();
    }
  }, 30_000);
});
