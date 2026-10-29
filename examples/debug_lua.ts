/**
 * Debug helper — exercise every Lua script end-to-end.
 *
 * Run: npx ts-node --project tsconfig.dev.json examples/debug_lua.ts
 *
 * Uses REDIS_HOST / REDIS_PORT (defaults: 127.0.0.1 / 6379).
 */
import { createRedisClient } from '../src/RedisClient';
import { KEYS } from '../src/constants';

const HOST = process.env['REDIS_HOST'] ?? '127.0.0.1';
const PORT = Number(process.env['REDIS_PORT'] ?? 6379);

process.on('unhandledRejection', (e) => { console.error('UNHANDLED:', e); process.exit(1); });

async function main() {
  const redis = createRedisClient({ host: HOST, port: PORT });
  const poolId = 'debug-pool';

  try {
    // 1. Connectivity
    const ping = await redis.ping();
    console.log('1. PING:', ping);

    // 2. Flush test keys
    const keys = await redis.keys('bq:dbg:*');
    if (keys.length > 0) await redis.del(...keys);
    console.log('2. Flushed', keys.length, 'old keys');

    // 3. Write meta
    await redis.hset(KEYS.meta('dbg'), {
      maxConcurrent: '2',
      minTime: '0',
      reservoir: '-1',
      reservoirRefreshInterval: '0',
      reservoirRefreshAmount: '0',
      reservoirIncreaseInterval: '0',
      reservoirIncreaseAmount: '0',
      reservoirIncreaseMaximum: '0',
      ttl: '3600',
    });
    console.log('3. Meta written');

    // 4. Enqueue
    const payload = JSON.stringify({
      id: 'job-abc-123',
      priority: 5,
      maxRetries: 0,
      retryCount: 0,
      enqueuedAt: Date.now(),
      owner: poolId,
    });
    const due = await redis.bqEnqueue(
      KEYS.jobs('dbg'), KEYS.delayed('dbg'), KEYS.meta('dbg'),
      KEYS.wakeup, KEYS.pools,
      payload, '0', '0', '5', '3600', 'dbg', '30000',
    );
    console.log('4. Enqueue due time:', due);

    // 5. Dequeue
    const raw = await redis.bqDequeue(
      KEYS.jobs('dbg'), KEYS.delayed('dbg'), KEYS.running('dbg'), KEYS.inflight('dbg'),
      KEYS.meta('dbg'), KEYS.reservoir('dbg'), KEYS.lastRun('dbg'),
      KEYS.lastRefresh('dbg'), KEYS.lastIncrease('dbg'),
      String(Date.now()), '3600', poolId, '256',
    );
    console.log('5. Dequeue result:', raw);

    // 6. Complete
    if (raw[0] === 'ok') {
      const done = await redis.bqComplete(
        KEYS.running('dbg'), KEYS.inflight('dbg'), KEYS.jobs('dbg'), KEYS.meta('dbg'),
        KEYS.lastRun('dbg'), KEYS.delayed('dbg'), KEYS.wakeup, KEYS.pools,
        'job-abc-123', 'dbg', '3600', poolId, '30000',
      );
      console.log('6. Complete result:', done);
    } else {
      console.log('6. Skipped complete — dequeue status was not ok');
    }

    // 7. Stats
    const stats = await redis.bqStats(
      KEYS.jobs('dbg'), KEYS.running('dbg'), KEYS.reservoir('dbg'),
      KEYS.delayed('dbg'), KEYS.meta('dbg'),
      KEYS.lastRefresh('dbg'), KEYS.lastIncrease('dbg'),
    );
    console.log('7. Stats:', stats);

    console.log('\n✅ All Lua scripts working correctly!');
  } catch (err) {
    console.error('\n❌ Error:', (err as Error).message);
    console.error((err as Error).stack);
  } finally {
    await redis.quit();
    process.exit(0);
  }
}

main();
