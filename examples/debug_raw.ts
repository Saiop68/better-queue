/**
 * Debug helper — raw ioredis test on a notification channel (no custom commands).
 *
 * Run: npx ts-node --project tsconfig.dev.json examples/debug_raw.ts
 *
 * Uses REDIS_HOST / REDIS_PORT (defaults: 127.0.0.1 / 6379).
 */
import Redis from 'ioredis';

const HOST = process.env['REDIS_HOST'] ?? '127.0.0.1';
const PORT = Number(process.env['REDIS_PORT'] ?? 6379);

// Workers listen on their own channel.
const CHANNEL = 'bq:workers:notify:debug-pool';

async function main() {
  const r = new Redis({ host: HOST, port: PORT });

  console.log('Connected. PING:', await r.ping());
  console.log('DBSIZE:', await r.dbsize());

  // Raw LPUSH
  const pushResult = await r.lpush(CHANNEL, 'hello');
  console.log('LPUSH result (should be 1+):', pushResult);

  const llen = await r.llen(CHANNEL);
  console.log(`LLEN ${CHANNEL}:`, llen);

  // Immediate RPOP
  const rpop = await r.rpop(CHANNEL);
  console.log('RPOP:', rpop);

  // BRPOP test with immediate push
  const brpopPromise = r.brpop(CHANNEL, 3);

  // Push after 100ms using a separate connection
  const r2 = new Redis({ host: HOST, port: PORT });
  setTimeout(async () => {
    const res = await r2.lpush(CHANNEL, 'from-r2');
    console.log('r2 LPUSH result:', res);
  }, 100);

  const brpopResult = await brpopPromise;
  console.log('BRPOP result:', brpopResult);

  await r.quit();
  await r2.quit();
  process.exit(0);
}

main().catch((e) => { console.error(e.message, e.stack); process.exit(1); });
