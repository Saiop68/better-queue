import Redis, { type RedisOptions } from 'ioredis';
import { LUA } from './lua-scripts';

// ── Typed Redis command extensions ─────────────────────────────────────────
declare module 'ioredis' {
  interface Redis {
    bqEnqueue(
      jobsKey: string,
      delayedKey: string,
      metaKey: string,
      wakeupKey: string,
      poolsKey: string,
      payload: string,
      scheduleAt: string,
      delayMs: string,
      priority: string,
      ttl: string,
      queueId: string,
      liveWindowMs: string,
    ): Promise<string>;

    bqDequeue(
      jobsKey: string,
      delayedKey: string,
      runningKey: string,
      inflightKey: string,
      metaKey: string,
      reservoirKey: string,
      lastRunKey: string,
      lastRefreshKey: string,
      lastIncreaseKey: string,
      now: string,
      ttl: string,
      ownerId: string,
      scanWindow: string,
    ): Promise<[string, string, string]>;

    bqComplete(
      runningKey: string,
      inflightKey: string,
      jobsKey: string,
      metaKey: string,
      lastRunKey: string,
      delayedKey: string,
      wakeupKey: string,
      poolsKey: string,
      jobId: string,
      queueId: string,
      ttl: string,
      ownerPoolId: string,
    ): Promise<number>;

    bqWakeupCheck(
      wakeupKey: string,
      poolsKey: string,
      now: string,
      batchSize: string,
      liveWindowMs: string,
    ): Promise<number>;

    bqRecover(
      inflightKey: string,
      runningKey: string,
      jobsKey: string,
      metaKey: string,
      wakeupKey: string,
      now: string,
      visibilityTimeout: string,
      queueId: string,
      ttl: string,
      ownerPoolId: string,
    ): Promise<number>;

    bqStats(
      jobsKey: string,
      runningKey: string,
      reservoirKey: string,
      delayedKey: string,
      metaKey: string,
      lastRefreshKey: string,
      lastIncreaseKey: string,
    ): Promise<[string, string, string]>;

    bqPoolHeartbeat(
      poolsKey: string,
      poolId: string,
      liveWindowMs: string,
    ): Promise<string>;

    bqPoolUnregister(
      poolsKey: string,
      notifyPoolKey: string,
      poolId: string,
    ): Promise<number>;
  }
}

/** Register all custom Lua commands on a Redis instance. */
function registerCommands(redis: Redis): void {
  redis.defineCommand('bqEnqueue', {
    numberOfKeys: 5, // jobs, delayed, meta, wakeup, pools
    lua: LUA.enqueue,
  });

  redis.defineCommand('bqDequeue', {
    numberOfKeys: 9, // jobs, delayed, running, inflight, meta, reservoir, lastRun, lastRefresh, lastIncrease
    lua: LUA.dequeue,
  });

  redis.defineCommand('bqComplete', {
    numberOfKeys: 8, // running, inflight, jobs, meta, lastRun, delayed, wakeup, pools
    lua: LUA.complete,
  });

  redis.defineCommand('bqWakeupCheck', {
    numberOfKeys: 2, // wakeup, pools
    lua: LUA.wakeupCheck,
  });

  redis.defineCommand('bqRecover', {
    numberOfKeys: 5, // inflight, running, jobs, meta, wakeup
    lua: LUA.recover,
  });

  redis.defineCommand('bqStats', {
    numberOfKeys: 7,
    lua: LUA.stats,
  });

  redis.defineCommand('bqPoolHeartbeat', {
    numberOfKeys: 1, // pools
    lua: LUA.poolHeartbeat,
  });

  redis.defineCommand('bqPoolUnregister', {
    numberOfKeys: 2, // pools, notifyPool
    lua: LUA.poolUnregister,
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
