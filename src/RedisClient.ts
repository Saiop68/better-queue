import Redis, { type RedisOptions } from 'ioredis';
import { LUA } from './lua-scripts';

// ── Typed Redis command extensions ─────────────────────────────────────────
declare module 'ioredis' {
  interface Redis {
    bqEnqueue(
      jobsKey: string,
      metaKey: string,
      notifyKey: string,
      payload: string,
      score: string,
      ttl: string,
      queueId: string,
    ): Promise<string>;

    bqDequeue(
      jobsKey: string,
      runningKey: string,
      inflightKey: string,
      metaKey: string,
      reservoirKey: string,
      lastRunKey: string,
      lastRefreshKey: string,
      now: string,
    ): Promise<[string, string, string]>;

    bqComplete(
      runningKey: string,
      inflightKey: string,
      jobsKey: string,
      notifyKey: string,
      metaKey: string,
      lastRunKey: string,
      jobId: string,
      queueId: string,
      ttl: string,
    ): Promise<number>;

    bqWakeupCheck(
      wakeupKey: string,
      notifyKey: string,
      now: string,
      batchSize: string,
    ): Promise<number>;

    bqRecover(
      inflightKey: string,
      runningKey: string,
      jobsKey: string,
      notifyKey: string,
      now: string,
      visibilityTimeout: string,
      queueId: string,
      ttl: string,
    ): Promise<number>;

    bqStats(
      jobsKey: string,
      runningKey: string,
      reservoirKey: string,
    ): Promise<[string, string, string]>;
  }
}

/** Register all custom Lua commands on a Redis instance. */
function registerCommands(redis: Redis): void {
  redis.defineCommand('bqEnqueue', {
    numberOfKeys: 3,
    lua: LUA.enqueue,
  });

  redis.defineCommand('bqDequeue', {
    numberOfKeys: 7,
    lua: LUA.dequeue,
  });

  redis.defineCommand('bqComplete', {
    numberOfKeys: 6,  // running, inflight, jobs, notify, meta, lastRun
    lua: LUA.complete,
  });

  redis.defineCommand('bqWakeupCheck', {
    numberOfKeys: 2,
    lua: LUA.wakeupCheck,
  });

  redis.defineCommand('bqRecover', {
    numberOfKeys: 4,
    lua: LUA.recover,
  });

  redis.defineCommand('bqStats', {
    numberOfKeys: 3,
    lua: LUA.stats,
  });
}

/**
 * Create a new ioredis connection with all BetterQueue custom commands
 * pre-registered. Every connection in the pool uses this factory.
 */
export function createRedisClient(options: RedisOptions): Redis {
  const client = new Redis({
    enableOfflineQueue: true,
    lazyConnect: false,
    maxRetriesPerRequest: 3,
    ...options,
  });

  registerCommands(client);

  client.on('error', (err: Error) => {
    // Surface errors without crashing — WorkerPool handles reconnection.
    process.stderr.write(`[BetterQueue] Redis error: ${err.message}\n`);
  });

  return client;
}

/**
 * Create a dedicated blocking connection for BRPOP.
 * This connection must NOT be shared with other commands.
 */
export function createBlockingClient(options: RedisOptions): Redis {
  return new Redis({
    enableOfflineQueue: true,
    lazyConnect: false,
    // No retries on blocking connection — ioredis auto-reconnects
    maxRetriesPerRequest: null,
    ...options,
  });
}
