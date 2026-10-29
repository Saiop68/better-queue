/**
 * Debug helper — verify that the per-pool BRPOP channel works.
 *
 * Run: npx ts-node --project tsconfig.dev.json examples/debug_brpop.ts
 *
 * Uses REDIS_HOST / REDIS_PORT (defaults: 127.0.0.1 / 6379).
 */
import { createBlockingClient, createRedisClient } from '../src/RedisClient';
import { KEYS } from '../src/constants';

const HOST = process.env['REDIS_HOST'] ?? '127.0.0.1';
const PORT = Number(process.env['REDIS_PORT'] ?? 6379);

async function main() {
  const pub = createRedisClient({ host: HOST, port: PORT });
  const sub = createBlockingClient({ host: HOST, port: PORT });

  // Each WorkerPool listens on its own channel.
  const channel = KEYS.notifyPool('debug-pool');

  console.log('pub ping:', await pub.ping());
  console.log('sub ping:', await sub.ping());
  console.log('listening on:', channel);

  let received = false;
  const waiter = sub.brpop(channel, 5).then((result) => {
    console.log('BRPOP result:', result);
    received = true;
    return result;
  });

  // Publish a message after a short delay
  await new Promise((r) => setTimeout(r, 200));
  console.log('Publishing to', channel);
  await pub.lpush(channel, 'test-queue-id');
  console.log('Published');

  await waiter;
  console.log('Received:', received);

  await pub.quit();
  await sub.quit();
  process.exit(0);
}

main().catch((e) => { console.error(e); process.exit(1); });
