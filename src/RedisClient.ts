import Redis, {
  Cluster,
  type RedisOptions,
  type ClusterOptions,
  type ClusterNode,
} from 'ioredis';
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
      liveWindowMs: string,
    ): Promise<number>;

    bqWakeupSchedule(
      wakeupKey: string,
      member: string,
      atMs: string,
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

  redis.defineCommand('bqWakeupSchedule', {
    numberOfKeys: 1, // wakeup
    lua: LUA.wakeupSchedule,
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

// ── Deployment configuration ───────────────────────────────────────────────

/**
 * A Sentinel-managed Redis deployment.
 *
 * The Sentinel discovers the current master and hands back a primary endpoint.
 * If the master fails over, `ioredis` reconnects to the new one.
 */
export interface SentinelOptions {
  /** Sentinel addresses to query for the master. */
  readonly sentinels: readonly ClusterNode[];
  /** The name the master is registered under, e.g. `mymaster`. */
  readonly name: string;
  /** Password for the Sentinel itself (usually different from the data node). */
  readonly sentinelPassword?: string;
  /** Username for the Sentinel itself. */
  readonly sentinelUsername?: string;
  /** Password for the master and its replicas. */
  readonly password?: string;
  /** Username for the master and its replicas. */
  readonly username?: string;
  /** TLS to the data nodes. */
  readonly tls?: Record<string, unknown>;
  /** Any other ioredis option applied to the data connection. */
  readonly redisOptions?: RedisOptions;
}

/**
 * How to reach Redis.
 *
 * - `{ host, port }` — a single node (the default, unchanged behaviour)
 * - `{ sentinels, name }` — a Sentinel-managed primary with automatic failover
 */
export type RedisConnection =
  | RedisOptions
  | SentinelOptions;

/**
 * Normalise the two accepted shapes into one ioredis options object.
 *
 * The existing `{ host, port }` form must keep working exactly as before,
 * so the single-node path is passed through untouched.
 */
function toIoredisOptions(connection: RedisConnection): RedisOptions {
  if (!isSentinelOptions(connection)) return connection;

  const { sentinels, name, sentinelPassword, sentinelUsername, password, username, tls, redisOptions } = connection;

  return {
    sentinels: sentinels as ClusterNode[],
    name,
    ...(sentinelPassword ? { sentinelPassword } : {}),
    ...(sentinelUsername ? { sentinelUsername } : {}),
    ...(password ? { password } : {}),
    ...(username ? { username } : {}),
    ...(tls ? { tls } : {}),
    ...(redisOptions ?? {}),
  } as RedisOptions;
}

/**
 * Does this look like a Sentinel configuration rather than plain ioredis
 * options?
 *
 * `sentinels` + `name` is the discriminator. Plain ioredis options never
 * carry both, and `name` alone would be ambiguous, so require both.
 */
export function isSentinelOptions(
  connection: RedisConnection,
): connection is SentinelOptions {
  return (
    typeof connection === 'object' &&
    connection !== null &&
    Array.isArray((connection as SentinelOptions).sentinels) &&
    (connection as SentinelOptions).sentinels.length > 0 &&
    typeof (connection as SentinelOptions).name === 'string' &&
    (connection as SentinelOptions).name.length > 0
  );
}

/**
 * Create a new ioredis connection with all BetterQueue custom commands
 * pre-registered. Every connection in the pool uses this factory.
 */
export function createRedisClient(options: RedisConnection): Redis {
  const client = new Redis({
    enableOfflineQueue: true,
    lazyConnect: false,
    maxRetriesPerRequest: 3,
    ...toIoredisOptions(options),
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
export function createBlockingClient(options: RedisConnection): Redis {
  return new Redis({
    enableOfflineQueue: true,
    lazyConnect: false,
    // No retries on blocking connection — ioredis auto-reconnects
    maxRetriesPerRequest: null,
    ...toIoredisOptions(options),
  });
}

/**
 * Re-export the Cluster type so callers can construct one and pass it in
 * without depending on ioredis directly.
 *
 * 📌 Cluster is NOT supported yet: the Lua scripts operate on several keys at
 * once and the key layout has no hash tag, so a clustered deployment returns
 * CROSSSLOT. See docs/CLUSTER-SUPPORT.md.
 */
export { Cluster };
export type { ClusterOptions };
