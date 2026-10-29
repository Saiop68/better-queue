/**
 * Child-process fixture for the cross-process regression test.
 *
 * Spawned as a REAL second Node process (see cross-process.test.ts) so the
 * two WorkerPools live in two different processes and actually compete for
 * the same Redis-backed concurrency slots.
 *
 * Usage: ts-node child-process.ts <role> <queueId> <holdMs> <delayMs> <port>
 *   role 'a' → schedules a job that holds the single concurrency slot.
 *   role 'b' → schedules a priority-1 job while the slot is held.
 *
 * Prints a single JSON line on stdout so the parent test can assert on it.
 * All timestamps are absolute wall-clock ms so the parent can compare the
 * two processes' clocks directly.
 */
import { WorkerPool, DistributedLimiter } from '../../src/index';

const argv = process.argv.slice(2);

if (argv.length < 5) {
  throw new Error('usage: child-process.ts <role> <queueId> <holdMs> <delayMs> <port>');
}

const role = argv[0] as string;
const queueId = argv[1] as string;
const holdMs = Number(argv[2]);
const delayMs = Number(argv[3]);
const port = Number(argv[4]);

const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

async function main(): Promise<void> {
  const pool = await WorkerPool.create({
    redis: { host: '127.0.0.1', port },
    workers: 2,
    tickIntervalMs: 25,
    pendingJobTimeoutMs: 0,
  });

  const limiter = new DistributedLimiter({ id: queueId, maxConcurrent: 1 });

  if (role === 'a') {
    let startedAt = 0;
    let finishedAt = 0;
    await limiter.schedule(async () => {
      startedAt = Date.now();
      await sleep(holdMs);
      finishedAt = Date.now();
    });
    process.stdout.write(
      JSON.stringify({
        role,
        ok: true,
        poolId: pool.poolId,
        startedAt,
        finishedAt,
      }) + '\n',
    );
  } else {
    await sleep(delayMs);
    const scheduledAt = Date.now();
    let startedAt = 0;
    const result = await limiter.schedule({ priority: 1 }, async () => {
      startedAt = Date.now();
      return 'RAN';
    });
    process.stdout.write(
      JSON.stringify({
        role,
        ok: true,
        poolId: pool.poolId,
        result,
        scheduledAt,
        startedAt,
      }) + '\n',
    );
  }

  await pool.stop();
  process.exit(0);
}

void main().catch((err: unknown) => {
  process.stdout.write(
    JSON.stringify({ role, ok: false, error: String(err) }) + '\n',
  );
  process.exit(1);
});
