import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import type Redis from 'ioredis';
import { DistributedLimiter } from '../src/index';
import {
  createTestClient,
  flushTestRedis,
  startPool,
  stopPool,
  sleep,
  waitFor,
} from './helpers';

let redis: Redis;
let seq = 0;

/** Unique queue id per test so suites never collide. */
function queueId(name: string): string {
  seq += 1;
  return `test:${name}:${seq}:${process.pid}`;
}

beforeAll(async () => {
  redis = createTestClient();
  await startPool();
});

afterAll(async () => {
  await stopPool();
  await flushTestRedis(redis);
  await redis.quit();
});

beforeEach(async () => {
  await flushTestRedis(redis);
});

// ─────────────────────────────────────────────────────────────────────────────
// 1. Basic scheduling
// ─────────────────────────────────────────────────────────────────────────────

describe('basic schedule', () => {
  it('runs a job and resolves with its return value', async () => {
    const limiter = new DistributedLimiter({ id: queueId('basic'), maxConcurrent: 1 });
    const result = await limiter.schedule(async () => 'hello');
    expect(result).toBe('hello');
  });

  it('runs several jobs concurrently and resolves all of them', async () => {
    const limiter = new DistributedLimiter({ id: queueId('basic-many'), maxConcurrent: 4 });
    const results = await Promise.all(
      Array.from({ length: 10 }, (_, i) => limiter.schedule(async () => i)),
    );
    expect(results).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
  });

  it('supports a void job', async () => {
    const limiter = new DistributedLimiter({ id: queueId('basic-void'), maxConcurrent: 1 });
    let ran = false;
    await limiter.schedule(async () => {
      ran = true;
    });
    expect(ran).toBe(true);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 2. maxConcurrent
// ─────────────────────────────────────────────────────────────────────────────

describe('maxConcurrent', () => {
  it('never exceeds the configured concurrency', async () => {
    const limiter = new DistributedLimiter({ id: queueId('conc'), maxConcurrent: 1 });

    let current = 0;
    let peak = 0;
    const tasks = Array.from({ length: 12 }, () =>
      limiter.schedule(async () => {
        current += 1;
        peak = Math.max(peak, current);
        await sleep(40);
        current -= 1;
      }),
    );

    await Promise.all(tasks);
    expect(peak).toBe(1);
  });

  it('allows exactly N jobs in parallel', async () => {
    const limiter = new DistributedLimiter({ id: queueId('conc3'), maxConcurrent: 3 });

    let current = 0;
    let peak = 0;
    await Promise.all(
      Array.from({ length: 15 }, () =>
        limiter.schedule(async () => {
          current += 1;
          peak = Math.max(peak, current);
          await sleep(30);
          current -= 1;
        }),
      ),
    );
    expect(peak).toBeLessThanOrEqual(3);
    expect(peak).toBeGreaterThan(1);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 3. minTime
// ─────────────────────────────────────────────────────────────────────────────

describe('minTime', () => {
  it('paces job starts by at least minTime', async () => {
    const limiter = new DistributedLimiter({
      id: queueId('mintime'),
      maxConcurrent: 5,
      minTime: 200,
    });

    const start = Date.now();
    await Promise.all([1, 2, 3].map(() => limiter.schedule(async () => Date.now() - start)));

    // 3 jobs with a 200ms gap between starts → at least ~400ms
    expect(Date.now() - start).toBeGreaterThanOrEqual(400);
  });

  it('does not delay a single job', async () => {
    const limiter = new DistributedLimiter({ id: queueId('mintime-1'), maxConcurrent: 1, minTime: 500 });
    const start = Date.now();
    await limiter.schedule(async () => 1);
    expect(Date.now() - start).toBeLessThan(400);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 4. Reservoir — refresh (reset) semantics
// ─────────────────────────────────────────────────────────────────────────────

describe('reservoir refresh', () => {
  it('allows a burst of `reservoir` jobs, then blocks until the refresh', async () => {
    const limiter = new DistributedLimiter({
      id: queueId('reservoir'),
      maxConcurrent: 1,
      reservoir: 3,
      reservoirRefreshInterval: 1_000,
      reservoirRefreshAmount: 3,
    });

    // Wait for the initial sync so the reservoir is seeded.
    await limiter.getStats();
    expect(await limiter.getReservoir()).toBe(3);

    const start = Date.now();
    await Promise.all(Array.from({ length: 5 }, () => limiter.schedule(async () => 1)));

    // 3 run immediately, the remaining 2 wait for the refresh (~1s later).
    expect(Date.now() - start).toBeGreaterThanOrEqual(900);
  });

  it('refills back to reservoirRefreshAmount', async () => {
    const limiter = new DistributedLimiter({
      id: queueId('reservoir-refill'),
      maxConcurrent: 1,
      reservoir: 2,
      reservoirRefreshInterval: 300,
      reservoirRefreshAmount: 2,
    });

    await limiter.getStats();
    expect(await limiter.getReservoir()).toBe(2);

    // Drain the bucket.
    await Promise.all([1, 2].map(() => limiter.schedule(async () => 1)));
    expect(await limiter.getReservoir()).toBe(0);

    // After the refresh interval the bucket is reset to the refresh amount.
    await waitFor(async () => (await limiter.getReservoir()) === 2, 5_000);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 5. Reservoir — increase (additive) semantics  [Bottleneck parity]
// ─────────────────────────────────────────────────────────────────────────────

describe('reservoir increase', () => {
  it('ADDS tokens on each interval instead of resetting them', async () => {
    const limiter = new DistributedLimiter({
      id: queueId('reservoir-increase'),
      maxConcurrent: 1,
      reservoir: 2,
      reservoirIncreaseInterval: 300,
      reservoirIncreaseAmount: 3,
    });

    await limiter.getStats();
    expect(await limiter.getReservoir()).toBe(2);

    // Consume the whole bucket.
    await Promise.all([1, 2].map(() => limiter.schedule(async () => 1)));
    expect(await limiter.getReservoir()).toBe(0);

    // One increase interval adds `reservoirIncreaseAmount` tokens (0 + 3 = 3).
    await waitFor(async () => (await limiter.getReservoir()) === 3, 5_000);
  });

  it('accumulates across multiple intervals', async () => {
    const limiter = new DistributedLimiter({
      id: queueId('reservoir-increase-accum'),
      maxConcurrent: 1,
      reservoir: 0,
      reservoirIncreaseInterval: 250,
      reservoirIncreaseAmount: 2,
    });

    await limiter.getStats();
    expect(await limiter.getReservoir()).toBe(0);

    // After ~2 intervals the bucket should have grown to 4 (2 + 2).
    await waitFor(async () => (await limiter.getReservoir()) === 4, 5_000);
  });

  it('respects reservoirIncreaseMaximum', async () => {
    const limiter = new DistributedLimiter({
      id: queueId('reservoir-increase-max'),
      maxConcurrent: 1,
      reservoir: 1,
      reservoirIncreaseInterval: 250,
      reservoirIncreaseAmount: 5,
      reservoirIncreaseMaximum: 3,
    });

    await limiter.getStats();

    // The cap keeps the bucket at 3 even though 5 tokens are added per tick.
    await waitFor(async () => (await limiter.getReservoir()) === 3, 5_000);
    await sleep(400);
    expect(await limiter.getReservoir()).toBe(3);
  });

  it('unblocks queued jobs once tokens arrive', async () => {
    const limiter = new DistributedLimiter({
      id: queueId('reservoir-increase-unblock'),
      maxConcurrent: 1,
      reservoir: 0,
      reservoirIncreaseInterval: 300,
      reservoirIncreaseAmount: 1,
    });

    await limiter.getStats();

    const start = Date.now();
    await Promise.all([1, 2, 3].map(() => limiter.schedule(async () => 1)));

    // 3 jobs, 1 token per 300ms → at least ~600ms for the last one.
    expect(Date.now() - start).toBeGreaterThanOrEqual(500);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 6. scheduleAt — delayed jobs must actually fire  [BUG 1]
// ─────────────────────────────────────────────────────────────────────────────

describe('scheduleAt', () => {
  it('runs a job scheduled in the future', async () => {
    const limiter = new DistributedLimiter({ id: queueId('scheduleat'), maxConcurrent: 1 });

    let fired = false;
    const start = Date.now();
    await limiter.schedule({ scheduleAt: Date.now() + 1_000 }, async () => {
      fired = true;
    });

    expect(fired).toBe(true);
    expect(Date.now() - start).toBeGreaterThanOrEqual(900);
  });

  it('does not run a delayed job before its due time', async () => {
    const limiter = new DistributedLimiter({ id: queueId('scheduleat-early'), maxConcurrent: 1 });

    let fired = false;
    const promise = limiter.schedule({ scheduleAt: Date.now() + 1_200 }, async () => {
      fired = true;
    });

    await sleep(400);
    expect(fired).toBe(false);

    await promise;
    expect(fired).toBe(true);
  });

  it('runs multiple delayed jobs', async () => {
    const limiter = new DistributedLimiter({ id: queueId('scheduleat-many'), maxConcurrent: 2 });

    const order: number[] = [];
    await Promise.all(
      [0, 1, 2].map((i) =>
        limiter.schedule({ scheduleAt: Date.now() + 400 + i * 300 }, async () => {
          order.push(i);
        }),
      ),
    );

    expect(order).toEqual([0, 1, 2]);
  });

  it('keeps a delayed job pending in Redis until it is due', async () => {
    const id = queueId('scheduleat-pending');
    const limiter = new DistributedLimiter({ id, maxConcurrent: 1 });

    const promise = limiter.schedule({ scheduleAt: Date.now() + 1_500 }, async () => 'done');
    await sleep(200);

    expect(await limiter.getQueueSize()).toBe(1);

    await promise;
    expect(await limiter.getQueueSize()).toBe(0);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 7. Priority ordering  [BUG 2]
// ─────────────────────────────────────────────────────────────────────────────

describe('priority', () => {
  it('runs a higher-priority job before a lower-priority one', async () => {
    const limiter = new DistributedLimiter({ id: queueId('priority'), maxConcurrent: 1 });

    const order: string[] = [];
    // Enqueue the LOWEST priority first — it must still run last.
    await Promise.all([
      limiter.schedule({ priority: 9 }, async () => {
        order.push('low');
      }),
      limiter.schedule({ priority: 1 }, async () => {
        order.push('high');
      }),
    ]);

    expect(order[0]).toBe('high');
    expect(order[order.length - 1]).toBe('low');
  });

  it('orders a full batch by priority', async () => {
    const limiter = new DistributedLimiter({ id: queueId('priority-batch'), maxConcurrent: 1 });

    const order: number[] = [];
    await Promise.all(
      [5, 1, 9, 3, 7].map((priority) =>
        limiter.schedule({ priority }, async () => {
          order.push(priority);
        }),
      ),
    );

    expect(order).toEqual([1, 3, 5, 7, 9]);
  });

  it('keeps FIFO order within the same priority', async () => {
    const limiter = new DistributedLimiter({ id: queueId('priority-fifo'), maxConcurrent: 1 });

    const order: number[] = [];
    await Promise.all(
      Array.from({ length: 5 }, (_, i) =>
        limiter.schedule({ priority: 4 }, async () => {
          order.push(i);
        }),
      ),
    );

    expect(order).toEqual([0, 1, 2, 3, 4]);
  });

  it('stores a priority-aware score in Redis', async () => {
    const id = queueId('priority-score');
    const limiter = new DistributedLimiter({ id, maxConcurrent: 1 });

    // A long-running blocker occupies the single concurrency slot, so the
    // two jobs below stay pending in the ZSET where we can inspect their
    // composed scores.
    const blocker = limiter.schedule(async () => {
      await sleep(1_500);
    });
    await waitFor(async () => (await redis.scard(`bq:${id}:running`)) === 1, 5_000);

    const first = limiter.schedule({ priority: 1 }, async () => 1);
    const second = limiter.schedule({ priority: 9 }, async () => 1);

    await waitFor(async () => (await redis.zcard(`bq:${id}:jobs`)) === 2, 5_000);

    const scores = await redis.zrange(`bq:${id}:jobs`, 0, -1, 'WITHSCORES');
    const nums = scores.filter((_, i) => i % 2 === 1).map(Number);

    // The priority-1 job must score lower than the priority-9 job.
    expect(nums.length).toBe(2);
    expect(nums[0]!).toBeLessThan(nums[1]!);

    await Promise.all([blocker, first, second]);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 8. Error handling + retry
// ─────────────────────────────────────────────────────────────────────────────

describe('errors and retries', () => {
  it('rejects the promise when the job throws and retries are disabled', async () => {
    const limiter = new DistributedLimiter({
      id: queueId('error'),
      maxConcurrent: 1,
      retryOnFailure: false,
      maxRetries: 0,
    });

    await expect(
      limiter.schedule(async () => {
        throw new Error('boom');
      }),
    ).rejects.toThrow('boom');
  });

  it('retries a failing job and eventually succeeds', async () => {
    const limiter = new DistributedLimiter({
      id: queueId('retry-ok'),
      maxConcurrent: 1,
      retryOnFailure: true,
      maxRetries: 3,
    });

    let attempts = 0;
    const result = await limiter.schedule(async () => {
      attempts += 1;
      if (attempts < 3) throw new Error('transient');
      return 'recovered';
    });

    expect(result).toBe('recovered');
    expect(attempts).toBe(3);
  });

  it('rejects with JobMaxRetriesError once retries are exhausted', async () => {
    const limiter = new DistributedLimiter({
      id: queueId('retry-exhausted'),
      maxConcurrent: 1,
      retryOnFailure: true,
      maxRetries: 1,
    });

    let attempts = 0;
    await expect(
      limiter.schedule(async () => {
        attempts += 1;
        throw new Error('always fails');
      }),
    ).rejects.toThrow(/failed after 2 attempt/);

    expect(attempts).toBe(2);
  });

  it('emits retry and failed events', async () => {
    const limiter = new DistributedLimiter({
      id: queueId('retry-events'),
      maxConcurrent: 1,
      retryOnFailure: true,
      maxRetries: 1,
    });

    const retries: number[] = [];
    const failures: unknown[] = [];
    limiter.on('retry', (_jobId, attempt) => retries.push(attempt));
    limiter.on('failed', (_jobId, err) => failures.push(err));

    await expect(
      limiter.schedule(async () => {
        throw new Error('nope');
      }),
    ).rejects.toThrow();

    expect(retries.length).toBeGreaterThanOrEqual(1);
    expect(failures.length).toBe(1);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 9. Stats
// ─────────────────────────────────────────────────────────────────────────────

describe('stats', () => {
  it('reports queue size, running count and reservoir', async () => {
    const limiter = new DistributedLimiter({
      id: queueId('stats'),
      maxConcurrent: 1,
      reservoir: 5,
    });

    await limiter.getStats();

    const stats = await limiter.getStats();
    expect(stats.queueSize).toBe(0);
    expect(stats.runningCount).toBe(0);
    expect(stats.reservoir).toBe(5);
  });

  it('returns null reservoir when no reservoir is configured', async () => {
    const limiter = new DistributedLimiter({ id: queueId('stats-nores'), maxConcurrent: 1 });
    expect(await limiter.getReservoir()).toBeNull();
  });

  it('counts pending jobs while they wait', async () => {
    const limiter = new DistributedLimiter({ id: queueId('stats-pending'), maxConcurrent: 1 });

    // NOTE: do NOT await these — they must stay pending while we assert.
    const blocker = limiter.schedule(async () => {
      await sleep(1_200);
    });
    await sleep(150);

    const second = limiter.schedule(async () => 1);
    const third = limiter.schedule(async () => 1);

    expect(await limiter.getQueueSize()).toBe(2);
    expect(await limiter.getRunningCount()).toBe(1);

    await Promise.all([blocker, second, third]);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// 10. drain
// ─────────────────────────────────────────────────────────────────────────────

describe('drain', () => {
  it('waits until every queued and running job has finished', async () => {
    const limiter = new DistributedLimiter({ id: queueId('drain'), maxConcurrent: 2 });

    let done = 0;
    await Promise.all(
      Array.from({ length: 8 }, () =>
        limiter.schedule(async () => {
          await sleep(50);
          done += 1;
        }),
      ),
    );

    await limiter.drain(10_000);
    expect(done).toBe(8);
    expect(await limiter.getQueueSize()).toBe(0);
    expect(await limiter.getRunningCount()).toBe(0);
  });

  it('resolves immediately when the queue is already empty', async () => {
    const limiter = new DistributedLimiter({ id: queueId('drain-empty'), maxConcurrent: 1 });
    await limiter.drain(5_000);
  });
});
