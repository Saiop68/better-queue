/**
 * Debug helper — verify that LPUSH/RPOP work on a notification channel.
 *
 * Run: npx ts-node --project tsconfig.dev.json examples/debug_list.ts
 *
 * Uses REDIS_HOST / REDIS_PORT (defaults: 127.0.0.1 / 6379).
 */
import { createRedisClient } from '../src/RedisClient';
import { KEYS } from '../src/constants';

const HOST = process.env['REDIS_HOST'] ?? '127.0.0.1';
const PORT = Number(process.env['REDIS_PORT'] ?? 6379);

async function main() {
  const r = createRedisClient({ host: HOST, port: PORT });
  const channel = KEYS.notifyPool('debug-pool');

  // Check current state
  const before = await r.llen(channel);
  console.log('LLEN before:', before);

  // Push something
  await r.lpush(channel, 'queue-test-1');
  await r.lpush(channel, 'queue-test-2');

  const after = await r.llen(channel);
  console.log('LLEN after push:', after);

  // Pop it back
  const popped = await r.rpop(channel);
  console.log('RPOP result:', popped);

  const final = await r.llen(channel);
  console.log('LLEN final:', final);

  console.log('Notify channel:', channel);

  await r.quit();
  process.exit(0);
}

main().catch((e) => { console.error(e); process.exit(1); });
