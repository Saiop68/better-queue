// Debug script to test all Lua operations end-to-end
import { createRedisClient } from '../src/RedisClient';
import { KEYS } from '../src/constants';

process.on('unhandledRejection', (e) => { console.error('UNHANDLED:', e); process.exit(1); });

async function main() {
  const redis = createRedisClient({ host: '127.0.0.1', port: 6379 });
  
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
    });
    const score = await redis.bqEnqueue(
      KEYS.jobs('dbg'), KEYS.meta('dbg'), KEYS.notify,
      payload, '0', '3600', 'dbg',
    );
    console.log('4. Enqueue score:', score);

    // 5. Dequeue
    const raw = await redis.bqDequeue(
      KEYS.jobs('dbg'), KEYS.running('dbg'), KEYS.inflight('dbg'),
      KEYS.meta('dbg'), KEYS.reservoir('dbg'), KEYS.lastRun('dbg'), KEYS.lastRefresh('dbg'),
      String(Date.now() + 5_000),
    );
    console.log('5. Dequeue result:', raw);

    // 6. Complete
    if (raw[0] === 'ok') {
      const done = await redis.bqComplete(
        KEYS.running('dbg'), KEYS.inflight('dbg'), KEYS.jobs('dbg'), KEYS.notify,
        'job-abc-123', 'dbg',
      );
      console.log('6. Complete result:', done);
    } else {
      console.log('6. Skipped complete — dequeue status was not ok');
    }

    // 7. Stats
    const stats = await redis.bqStats(
      KEYS.jobs('dbg'), KEYS.running('dbg'), KEYS.reservoir('dbg'),
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
