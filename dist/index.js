"use strict";
var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);

// src/index.ts
var index_exports = {};
__export(index_exports, {
  BetterQueueError: () => BetterQueueError,
  DistributedLimiter: () => DistributedLimiter,
  JobMaxRetriesError: () => JobMaxRetriesError,
  KEYS: () => KEYS,
  LimiterManager: () => LimiterManager,
  LimiterStoppedError: () => LimiterStoppedError,
  LimiterWrapper: () => LimiterWrapper,
  RedisCommandError: () => RedisCommandError,
  WorkerPool: () => WorkerPool,
  WorkerPoolNotInitializedError: () => WorkerPoolNotInitializedError,
  limiterManager: () => limiterManager
});
module.exports = __toCommonJS(index_exports);

// src/WorkerPool.ts
var import_node_events = require("events");

// src/RedisClient.ts
var import_ioredis = __toESM(require("ioredis"));

// src/lua-scripts.ts
var LUA = {
  complete: `--[[
  complete.lua \u2014 Mark a job as complete, release its running slot, and
                 re-signal workers if more jobs exist in the queue.

  When the queue becomes fully empty (no pending, no running) all of its
  volatile keys are immediately deleted so Redis doesn't accumulate stale
  empty structures for inactive guilds.  This is the "idle cleanup" path \u2014
  the queue meta key is left alive (so config survives cache-miss recreation)
  but with a short TTL refreshed on every touch.

  KEYS[1] = bq:{id}:running     (SET  \u2014 currently running job IDs)
  KEYS[2] = bq:{id}:inflight    (HASH \u2014 jobId \u2192 payload, crash recovery)
  KEYS[3] = bq:{id}:jobs        (ZSET \u2014 pending jobs, checked for re-signal)
  KEYS[4] = bq:workers:notify   (LIST \u2014 worker signal channel)
  KEYS[5] = bq:{id}:meta        (HASH \u2014 queue config, TTL refreshed on activity)
  KEYS[6] = bq:{id}:lastRun     (STRING \u2014 timestamp of last job dequeue)

  ARGV[1] = jobId               (string)
  ARGV[2] = queueId             (string \u2014 for re-notification)
  ARGV[3] = ttl                 (seconds \u2014 key expiry window)

  Returns: 1 if job was found and removed, 0 if job was not in running set
--]]

local runningKey  = KEYS[1]
local inflightKey = KEYS[2]
local jobsKey     = KEYS[3]
local notifyKey   = KEYS[4]
local metaKey     = KEYS[5]
local lastRunKey  = KEYS[6]

local jobId   = ARGV[1]
local queueId = ARGV[2]
local ttl     = tonumber(ARGV[3])

local removed = redis.call('SREM', runningKey, jobId)
redis.call('HDEL', inflightKey, jobId)

if removed == 1 then
  local pending = redis.call('ZCARD', jobsKey)
  local running = redis.call('SCARD', runningKey)

  if pending > 0 then
    -- More jobs waiting \u2014 signal workers and refresh activity TTLs
    redis.call('LPUSH', notifyKey, queueId)
    redis.call('EXPIRE', metaKey,  ttl)
    redis.call('EXPIRE', lastRunKey, ttl)
    redis.call('EXPIRE', runningKey, ttl)
    redis.call('EXPIRE', inflightKey, ttl)
    redis.call('EXPIRE', jobsKey, ttl)
  elseif running == 0 then
    -- Queue is completely idle \u2014 DELETE volatile runtime keys immediately.
    -- This frees Redis memory for inactive guilds without waiting for TTL.
    -- meta key stays alive (short TTL) so getLimiter() cold-starts cheaply.
    redis.call('DEL', runningKey)
    redis.call('DEL', inflightKey)
    redis.call('DEL', jobsKey)
    redis.call('DEL', lastRunKey)
    -- Meta survives with its current TTL \u2014 cheap to recreate, avoids syncMeta
    -- on every single member-join for guilds that are somewhat active.
    redis.call('EXPIRE', metaKey, ttl)
  end
end

return removed
`,
  dequeue: `--[[
  dequeue.lua \u2014 Atomically fetch the next eligible job from a queue,
                enforcing maxConcurrent, minTime, and reservoir constraints.
                Performs lazy reservoir refresh (no timers needed).

  KEYS[1] = bq:{id}:jobs         (ZSET   \u2014 pending jobs)
  KEYS[2] = bq:{id}:running      (SET    \u2014 currently running job IDs)
  KEYS[3] = bq:{id}:inflight     (HASH   \u2014 jobId \u2192 payload, for crash recovery)
  KEYS[4] = bq:{id}:meta         (HASH   \u2014 queue configuration)
  KEYS[5] = bq:{id}:reservoir    (STRING \u2014 current token count)
  KEYS[6] = bq:{id}:lastRun      (STRING \u2014 ms timestamp of last dequeue)
  KEYS[7] = bq:{id}:lastRefresh  (STRING \u2014 ms timestamp of last reservoir refresh)

  ARGV[1] = now                  (current time in ms)

  Returns: table { status, payload, nextEligibleMs }
    status values:
      "ok"               \u2014 payload contains job JSON, nextEligibleMs = "0"
      "empty"            \u2014 no jobs scheduled for now
      "concurrent_full"  \u2014 maxConcurrent reached, nextEligibleMs = "0"
      "rate_limited"     \u2014 minTime or reservoir not ready, nextEligibleMs = when to retry
--]]

local jobsKey        = KEYS[1]
local runningKey     = KEYS[2]
local inflightKey    = KEYS[3]
local metaKey        = KEYS[4]
local reservoirKey   = KEYS[5]
local lastRunKey     = KEYS[6]
local lastRefreshKey = KEYS[7]

-- Use Redis server time as the authoritative clock for job scheduling.
-- This prevents clock-skew between the Node.js client and the Redis server
-- from causing jobs to be missed (enqueued with server time, dequeued with client time).
local _time   = redis.call('TIME')
local nowMs   = tonumber(_time[1]) * 1000 + math.floor(tonumber(_time[2]) / 1000)
-- ARGV[1] (client now) is still used for rate-limit timestamp comparisons where
-- sub-ms precision doesn't matter and approximate client time is fine.
local now = nowMs

-- \u2500\u2500 Load queue meta \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
local metaArr = redis.call('HGETALL', metaKey)
local meta = {}
for i = 1, #metaArr, 2 do
  meta[metaArr[i]] = metaArr[i + 1]
end

local maxConcurrent       = tonumber(meta['maxConcurrent'] or '0')
local minTime             = tonumber(meta['minTime'] or '0')
local ttl                 = tonumber(meta['ttl'] or '86400')
local refreshInterval     = tonumber(meta['reservoirRefreshInterval'] or '0')
local refreshAmount       = tonumber(meta['reservoirRefreshAmount'] or '0')
local hasReservoir        = meta['reservoir'] ~= nil and meta['reservoir'] ~= '-1'

-- \u2500\u2500 Lazy reservoir refresh \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
-- Instead of a timer, we check on every dequeue attempt whether a refresh
-- interval has elapsed and top-up the token bucket atomically.
if hasReservoir and refreshInterval > 0 and refreshAmount > 0 then
  local lastRefresh = tonumber(redis.call('GET', lastRefreshKey) or '0')
  if (now - lastRefresh) >= refreshInterval then
    redis.call('SET', reservoirKey, tostring(refreshAmount))
    redis.call('EXPIRE', reservoirKey, ttl)
    redis.call('SET', lastRefreshKey, tostring(now))
    redis.call('EXPIRE', lastRefreshKey, ttl)
  end
end

-- \u2500\u2500 Check maxConcurrent \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
if maxConcurrent > 0 then
  local running = tonumber(redis.call('SCARD', runningKey))
  if running >= maxConcurrent then
    return { 'concurrent_full', '', '0' }
  end
end

-- \u2500\u2500 Check minTime \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
local nextByMinTime = 0
if minTime > 0 then
  local lastRun = tonumber(redis.call('GET', lastRunKey) or '0')
  local elapsed = now - lastRun
  if elapsed < minTime then
    nextByMinTime = lastRun + minTime
    return { 'rate_limited', '', tostring(nextByMinTime) }
  end
end

-- \u2500\u2500 Check reservoir \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
if hasReservoir then
  local tokens = tonumber(redis.call('GET', reservoirKey) or '0')
  if tokens <= 0 then
    -- Calculate when the next refresh will happen
    local lastRefresh = tonumber(redis.call('GET', lastRefreshKey) or '0')
    local nextRefresh = lastRefresh + refreshInterval
    return { 'rate_limited', '', tostring(nextRefresh) }
  end
  -- Consume one token (will be atomically decremented below after job fetch)
end

-- \u2500\u2500 Fetch next due job \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
local jobs = redis.call('ZRANGEBYSCORE', jobsKey, '-inf', tostring(now), 'LIMIT', 0, 1)
if #jobs == 0 then
  return { 'empty', '', '0' }
end

local payload = jobs[1]

-- \u2500\u2500 Consume reservoir token (after confirming a job exists) \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
if hasReservoir then
  redis.call('DECR', reservoirKey)
end

-- \u2500\u2500 Move job to inflight tracking \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
redis.call('ZREM', jobsKey, payload)

-- Extract jobId \u2014 payload is JSON, parse the "id" field via pattern match
-- Pattern: ..."id":"<uuid>"... where uuid is alphanumeric-dash
local jobId = string.match(payload, '"id"%s*:%s*"([^"]+)"')

if jobId then
  redis.call('SADD', runningKey, jobId)
  redis.call('EXPIRE', runningKey, ttl)
  redis.call('HSET', inflightKey, jobId, payload)
  redis.call('EXPIRE', inflightKey, ttl)
end

-- Update lastRun timestamp
redis.call('SET', lastRunKey, tostring(now))
redis.call('EXPIRE', lastRunKey, ttl)

return { 'ok', payload, '0' }
`,
  enqueue: `--[[
  enqueue.lua \u2014 Atomically enqueue a job into a queue's ZSET and signal workers.

  KEYS[1] = bq:{id}:jobs        (ZSET \u2014 pending jobs)
  KEYS[2] = bq:{id}:meta        (HASH  \u2014 queue config)
  KEYS[3] = bq:workers:notify   (LIST  \u2014 worker signal channel)

  ARGV[1] = jobPayload          (JSON string)
  ARGV[2] = score               (ms timestamp, 0 = use Redis server time)
  ARGV[3] = ttl                 (seconds for key expiry)
  ARGV[4] = queueId             (string \u2014 pushed to notify list)

  Returns: actual score used (string)
--]]

local jobsKey   = KEYS[1]
local metaKey   = KEYS[2]
local notifyKey = KEYS[3]

local payload = ARGV[1]
local score   = tonumber(ARGV[2])
local ttl     = tonumber(ARGV[3])
local queueId = ARGV[4]

-- Use Redis server time if score is 0 (enqueue now)
if score == 0 then
  local t = redis.call('TIME')
  score = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

-- Add job to queue
redis.call('ZADD', jobsKey, score, payload)
redis.call('EXPIRE', jobsKey, ttl)

-- Refresh meta TTL (touch active queue)
redis.call('EXPIRE', metaKey, ttl)

-- Signal a worker that work is available
redis.call('LPUSH', notifyKey, queueId)

return tostring(score)
`,
  recover: `--[[
  recover.lua \u2014 On startup, re-enqueue any jobs from the inflight HASH that
                are older than a visibility timeout (likely orphaned by a crash).
                Designed to be called once at startup per known queue.

  KEYS[1] = bq:{id}:inflight    (HASH \u2014 jobId \u2192 payload)
  KEYS[2] = bq:{id}:running     (SET  \u2014 running job IDs)
  KEYS[3] = bq:{id}:jobs        (ZSET \u2014 pending jobs)
  KEYS[4] = bq:workers:notify   (LIST \u2014 worker signal channel)

  ARGV[1] = now                 (current time in ms)
  ARGV[2] = visibilityTimeout   (ms \u2014 jobs older than this are considered orphaned)
  ARGV[3] = queueId             (string)
  ARGV[4] = ttl                 (seconds for key expiry)

  Returns: number of jobs re-queued
--]]

local inflightKey = KEYS[1]
local runningKey  = KEYS[2]
local jobsKey     = KEYS[3]
local notifyKey   = KEYS[4]

local now               = tonumber(ARGV[1])
local visibilityTimeout = tonumber(ARGV[2])
local queueId           = ARGV[3]
local ttl               = tonumber(ARGV[4])

-- Get all inflight jobs (jobId \u2192 payload pairs)
local inflight = redis.call('HGETALL', inflightKey)
local requeued = 0

for i = 1, #inflight, 2 do
  local jobId   = inflight[i]
  local payload = inflight[i + 1]

  -- Extract enqueuedAt from JSON payload to check age
  -- Uses pattern: ..."enqueuedAt":<number>...
  local enqueuedAt = tonumber(string.match(payload, '"enqueuedAt"%s*:%s*(%d+)'))
  local isOrphaned = (enqueuedAt == nil) or ((now - enqueuedAt) > visibilityTimeout)

  if isOrphaned then
    -- Remove from inflight + running
    redis.call('HDEL', inflightKey, jobId)
    redis.call('SREM', runningKey, jobId)

    -- Extract retryCount and maxRetries from payload
    local retryCount = tonumber(string.match(payload, '"retryCount"%s*:%s*(%d+)')) or 0
    local maxRetries = tonumber(string.match(payload, '"maxRetries"%s*:%s*(%d+)')) or 0

    if retryCount < maxRetries then
      -- Bump retry count in payload
      local newPayload = string.gsub(payload, '"retryCount"%s*:%s*%d+', '"retryCount":' .. tostring(retryCount + 1), 1)
      -- Re-enqueue immediately (score = now)
      redis.call('ZADD', jobsKey, tostring(now), newPayload)
      redis.call('EXPIRE', jobsKey, ttl)
      redis.call('LPUSH', notifyKey, queueId)
      requeued = requeued + 1
    end
    -- If maxRetries exhausted, job is dropped (no fn to call anyway after crash)
  end
end

return requeued
`,
  stats: `--[[
  stats.lua \u2014 Atomically read queue statistics from Redis.

  KEYS[1] = bq:{id}:jobs        (ZSET   \u2014 pending jobs)
  KEYS[2] = bq:{id}:running     (SET    \u2014 running job IDs)
  KEYS[3] = bq:{id}:reservoir   (STRING \u2014 current token count)

  Returns: table { queueSize, runningCount, reservoir }
    reservoir is "-1" if not configured
--]]

local jobsKey      = KEYS[1]
local runningKey   = KEYS[2]
local reservoirKey = KEYS[3]

local queueSize    = redis.call('ZCARD', jobsKey)
local runningCount = redis.call('SCARD', runningKey)
local reservoir    = redis.call('GET', reservoirKey)

return {
  tostring(queueSize),
  tostring(runningCount),
  reservoir or '-1',
}
`,
  wakeupCheck: `--[[
  wakeup_check.lua \u2014 Drain the wakeup ZSET and signal workers for any
                     queues whose scheduled retry time has arrived.
                     Called by the single global ticker in WorkerPool.
                     This replaces ALL per-limiter timers.

  KEYS[1] = bq:global:wakeup    (ZSET \u2014 queueId \u2192 wakeup timestamp)
  KEYS[2] = bq:workers:notify   (LIST \u2014 worker signal channel)

  ARGV[1] = now                 (current time in ms)
  ARGV[2] = batchSize           (max queues to process per tick, e.g. "100")

  Returns: number of queues signalled
--]]

local wakeupKey = KEYS[1]
local notifyKey = KEYS[2]

local now       = ARGV[1]
local batchSize = tonumber(ARGV[2]) or 100

-- Fetch all queue IDs with wakeup time <= now (up to batchSize)
local queues = redis.call('ZRANGEBYSCORE', wakeupKey, '-inf', now, 'LIMIT', 0, batchSize)

if #queues == 0 then
  return 0
end

-- Remove processed entries from wakeup set
redis.call('ZREMRANGEBYSCORE', wakeupKey, '-inf', now)

-- Signal workers for each eligible queue
for _, queueId in ipairs(queues) do
  redis.call('LPUSH', notifyKey, queueId)
end

return #queues
`
};

// src/RedisClient.ts
function registerCommands(redis) {
  redis.defineCommand("bqEnqueue", {
    numberOfKeys: 3,
    lua: LUA.enqueue
  });
  redis.defineCommand("bqDequeue", {
    numberOfKeys: 7,
    lua: LUA.dequeue
  });
  redis.defineCommand("bqComplete", {
    numberOfKeys: 6,
    // running, inflight, jobs, notify, meta, lastRun
    lua: LUA.complete
  });
  redis.defineCommand("bqWakeupCheck", {
    numberOfKeys: 2,
    lua: LUA.wakeupCheck
  });
  redis.defineCommand("bqRecover", {
    numberOfKeys: 4,
    lua: LUA.recover
  });
  redis.defineCommand("bqStats", {
    numberOfKeys: 3,
    lua: LUA.stats
  });
}
function createRedisClient(options) {
  const client = new import_ioredis.default({
    enableOfflineQueue: true,
    lazyConnect: false,
    maxRetriesPerRequest: 3,
    ...options
  });
  registerCommands(client);
  client.on("error", (err) => {
    process.stderr.write(`[BetterQueue] Redis error: ${err.message}
`);
  });
  return client;
}
function createBlockingClient(options) {
  return new import_ioredis.default({
    enableOfflineQueue: true,
    lazyConnect: false,
    // No retries on blocking connection — ioredis auto-reconnects
    maxRetriesPerRequest: null,
    ...options
  });
}

// src/constants.ts
var KEYS = {
  /** ZSET — pending jobs, score = earliest allowed start time (ms) */
  jobs: (id) => `bq:${id}:jobs`,
  /** SET — job IDs of currently executing jobs */
  running: (id) => `bq:${id}:running`,
  /** HASH — jobId → full payload, for crash recovery */
  inflight: (id) => `bq:${id}:inflight`,
  /** HASH — queue configuration (maxConcurrent, minTime, etc.) */
  meta: (id) => `bq:${id}:meta`,
  /** STRING (int) — current token count for reservoir */
  reservoir: (id) => `bq:${id}:reservoir`,
  /** STRING (ms) — timestamp of last job dequeue */
  lastRun: (id) => `bq:${id}:lastRun`,
  /** STRING (ms) — timestamp of last reservoir refresh */
  lastRefresh: (id) => `bq:${id}:lastRefresh`,
  /** LIST — worker notification channel (BRPOP) */
  notify: "bq:workers:notify",
  /** ZSET — scheduled worker wakeups, score = wakeup time (ms) */
  wakeup: "bq:global:wakeup"
};
var DEFAULT_TTL_SECONDS = 3600;
var DEFAULT_WORKER_COUNT = 64;
var DEFAULT_TICK_INTERVAL_MS = 50;
var DEFAULT_VISIBILITY_TIMEOUT_MS = 5 * 60 * 1e3;
var MAX_DRAIN_PER_NOTIFY = 32;
var BRPOP_TIMEOUT_SECONDS = 1;
var DEFAULT_PRIORITY = 5;
var DEFAULT_MAX_RETRIES = 3;
var WAKEUP_BATCH_SIZE = 100;

// src/errors.ts
var BetterQueueError = class extends Error {
  constructor(message) {
    super(message);
    this.name = "BetterQueueError";
  }
};
var JobMaxRetriesError = class extends BetterQueueError {
  constructor(jobId, attempts, cause) {
    super(`Job ${jobId} failed after ${attempts} attempt(s).`);
    this.jobId = jobId;
    this.attempts = attempts;
    this.cause = cause;
    this.name = "JobMaxRetriesError";
  }
  jobId;
  attempts;
  cause;
};
var LimiterStoppedError = class extends BetterQueueError {
  constructor(queueId) {
    super(`Limiter "${queueId}" has been stopped and is no longer accepting jobs.`);
    this.queueId = queueId;
    this.name = "LimiterStoppedError";
  }
  queueId;
};
var WorkerPoolNotInitializedError = class extends BetterQueueError {
  constructor() {
    super(
      "WorkerPool has not been started. Call WorkerPool.create() before creating limiters."
    );
    this.name = "WorkerPoolNotInitializedError";
  }
};
var RedisCommandError = class extends BetterQueueError {
  constructor(command, cause) {
    super(`Redis command "${command}" failed: ${String(cause)}`);
    this.name = "RedisCommandError";
    this.cause = cause;
  }
};

// src/WorkerPool.ts
var globalPool = null;
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
function parseDequeueResult(raw) {
  return {
    status: raw[0],
    payload: raw[1] ?? "",
    nextEligibleMs: parseInt(raw[2] ?? "0", 10)
  };
}
var Worker = class {
  id;
  blockingConn;
  pool;
  running = false;
  constructor(id, redisOptions, pool) {
    this.id = id;
    this.pool = pool;
    this.blockingConn = createBlockingClient(redisOptions);
  }
  start() {
    this.running = true;
    void this.loop();
  }
  async stop() {
    this.running = false;
    await this.blockingConn.quit();
  }
  async loop() {
    while (this.running) {
      try {
        const result = await this.blockingConn.brpop(
          KEYS.notify,
          BRPOP_TIMEOUT_SECONDS
        );
        if (!result) continue;
        const queueId = result[1];
        if (queueId) await this.drainQueue(queueId);
      } catch (err) {
        if (this.running) {
          await sleep(200);
        }
      }
    }
  }
  /**
   * Drain as many jobs as possible from a queue in one burst.
   * Stops when the queue is empty, rate-limited, or max drain count reached.
   */
  async drainQueue(queueId) {
    for (let i = 0; i < MAX_DRAIN_PER_NOTIFY; i++) {
      const result = await this.tryDequeue(queueId);
      if (!result) break;
      const { status, payload, nextEligibleMs } = result;
      if (status === "ok" && payload) {
        await this.executeJob(queueId, payload);
        continue;
      }
      if (status === "rate_limited" && nextEligibleMs > 0) {
        await this.scheduleWakeup(queueId, nextEligibleMs);
      }
      break;
    }
  }
  async tryDequeue(queueId) {
    try {
      const raw = await this.pool.commandClient.bqDequeue(
        KEYS.jobs(queueId),
        KEYS.running(queueId),
        KEYS.inflight(queueId),
        KEYS.meta(queueId),
        KEYS.reservoir(queueId),
        KEYS.lastRun(queueId),
        KEYS.lastRefresh(queueId),
        String(Date.now())
      );
      return parseDequeueResult(raw);
    } catch (err) {
      process.stderr.write(`[BetterQueue] dequeue error for queue "${queueId}": ${String(err)}
`);
      return null;
    }
  }
  async executeJob(queueId, payloadJson) {
    let job;
    try {
      job = JSON.parse(payloadJson);
    } catch {
      return;
    }
    const pending = this.pool.pendingJobs.get(job.id);
    if (!pending) {
      await this.completeJob(queueId, job.id);
      return;
    }
    this.pool.emit("executing", job.id);
    try {
      const value = await pending.fn();
      this.pool.pendingJobs.delete(job.id);
      await this.completeJob(queueId, job.id);
      pending.resolve(value);
      this.pool.emit("done", job.id);
    } catch (err) {
      await this.handleJobError(queueId, job, pending, err);
    }
  }
  async handleJobError(queueId, job, pending, err) {
    const attempt = job.retryCount + 1;
    if (job.retryCount < job.maxRetries) {
      const delayMs = Math.min(1e3 * 2 ** job.retryCount, 3e4);
      const scheduleAt = Date.now() + delayMs;
      const retryPayload = { ...job, retryCount: attempt };
      this.pool.emit("retry", job.id, attempt);
      try {
        await this.pool.commandClient.bqEnqueue(
          KEYS.jobs(queueId),
          KEYS.meta(queueId),
          KEYS.notify,
          JSON.stringify(retryPayload),
          String(scheduleAt),
          String(this.pool.defaultTtl),
          queueId
        );
      } catch {
        this.pool.pendingJobs.delete(job.id);
        const retryErr = new JobMaxRetriesError(job.id, attempt, err);
        pending.reject(retryErr);
        this.pool.emit("failed", job.id, retryErr);
        return;
      }
      await this.completeJob(queueId, job.id);
    } else {
      this.pool.pendingJobs.delete(job.id);
      await this.completeJob(queueId, job.id);
      const finalErr = new JobMaxRetriesError(job.id, attempt, err);
      pending.reject(finalErr);
      this.pool.emit("failed", job.id, finalErr);
    }
  }
  async completeJob(queueId, jobId) {
    try {
      await this.pool.commandClient.bqComplete(
        KEYS.running(queueId),
        KEYS.inflight(queueId),
        KEYS.jobs(queueId),
        KEYS.notify,
        KEYS.meta(queueId),
        // NEW: for TTL refresh + idle cleanup signal
        KEYS.lastRun(queueId),
        // NEW: deleted immediately when queue goes idle
        jobId,
        queueId,
        String(this.pool.defaultTtl)
      );
    } catch {
    }
  }
  async scheduleWakeup(queueId, atMs) {
    try {
      await this.pool.commandClient.zadd(KEYS.wakeup, "NX", atMs, queueId);
    } catch {
    }
  }
};
var WorkerPool = class _WorkerPool extends import_node_events.EventEmitter {
  /** Shared command connection (non-blocking) */
  commandClient;
  /**
   * In-memory map of job closures. The ONLY mutable in-process state.
   * Entry lifecycle: added in schedule() → deleted in executeJob() or handleJobError().
   * Size is bounded by the number of concurrently running + pending-but-not-yet-started jobs.
   */
  pendingJobs = /* @__PURE__ */ new Map();
  defaultTtl;
  workers;
  redisOptions;
  tickIntervalMs;
  visibilityTimeoutMs;
  tickerHandle = null;
  stopped = false;
  constructor(options) {
    super();
    const workerCount = options.workers ?? DEFAULT_WORKER_COUNT;
    this.tickIntervalMs = options.tickIntervalMs ?? DEFAULT_TICK_INTERVAL_MS;
    this.visibilityTimeoutMs = options.visibilityTimeoutMs ?? DEFAULT_VISIBILITY_TIMEOUT_MS;
    this.defaultTtl = 86400;
    this.redisOptions = options.redis;
    this.commandClient = createRedisClient(options.redis);
    this.workers = Array.from(
      { length: workerCount },
      (_, i) => new Worker(i, options.redis, this)
    );
  }
  /**
   * Create and start the global WorkerPool.
   * Must be called once before any `DistributedLimiter` is created.
   *
   * @example
   * const pool = await WorkerPool.create({ redis: { host: 'localhost', port: 6379 } });
   */
  static async create(options) {
    if (globalPool) return globalPool;
    const pool = new _WorkerPool(options);
    await pool.start();
    globalPool = pool;
    return pool;
  }
  /** Returns the active global pool, or throws if not initialized. */
  static getInstance() {
    if (!globalPool) throw new WorkerPoolNotInitializedError();
    return globalPool;
  }
  async start() {
    for (const worker of this.workers) {
      worker.start();
    }
    this.startTicker();
  }
  /**
   * Gracefully stop the pool.
   * Waits for all in-flight jobs to complete, then closes connections.
   */
  async stop() {
    if (this.stopped) return;
    this.stopped = true;
    if (this.tickerHandle) {
      clearInterval(this.tickerHandle);
      this.tickerHandle = null;
    }
    await Promise.all(this.workers.map((w) => w.stop()));
    await this.commandClient.quit();
    if (globalPool === this) globalPool = null;
  }
  /**
   * Run crash recovery for a queue — re-enqueue orphaned inflight jobs.
   * Called automatically by DistributedLimiter on first use.
   */
  async recoverQueue(queueId, ttl) {
    try {
      return await this.commandClient.bqRecover(
        KEYS.inflight(queueId),
        KEYS.running(queueId),
        KEYS.jobs(queueId),
        KEYS.notify,
        String(Date.now()),
        String(this.visibilityTimeoutMs),
        queueId,
        String(ttl)
      );
    } catch {
      return 0;
    }
  }
  startTicker() {
    const tick = () => {
      void this.commandClient.bqWakeupCheck(
        KEYS.wakeup,
        KEYS.notify,
        String(Date.now()),
        String(WAKEUP_BATCH_SIZE)
      ).catch(() => {
      });
    };
    this.tickerHandle = setInterval(tick, this.tickIntervalMs);
    this.tickerHandle.unref();
  }
};

// src/LimiterManager.ts
var import_lru_cache = require("lru-cache");

// src/DistributedLimiter.ts
var import_node_crypto = require("crypto");
var import_node_events2 = require("events");
var DistributedLimiter = class extends import_node_events2.EventEmitter {
  opts;
  pool;
  stopped = false;
  metaSynced = false;
  recoveryDone = false;
  constructor(options) {
    super();
    this.pool = WorkerPool.getInstance();
    this.opts = {
      id: options.id,
      maxConcurrent: options.maxConcurrent ?? 1,
      minTime: options.minTime ?? 0,
      reservoir: options.reservoir ?? -1,
      reservoirRefreshInterval: options.reservoirRefreshInterval ?? 0,
      reservoirRefreshAmount: options.reservoirRefreshAmount ?? 0,
      priority: options.priority ?? DEFAULT_PRIORITY,
      ttl: options.ttl ?? DEFAULT_TTL_SECONDS,
      retryOnFailure: options.retryOnFailure ?? true,
      maxRetries: options.maxRetries ?? DEFAULT_MAX_RETRIES
    };
  }
  /** The unique queue identifier. */
  get id() {
    return this.opts.id;
  }
  // ── Core API ───────────────────────────────────────────────────────────────
  /**
   * Schedule an async function to run under this limiter's rate limits.
   *
   * The function is enqueued in Redis and will be executed by a WorkerPool
   * worker when rate-limit conditions are satisfied. Returns a Promise that
   * resolves/rejects with the function's result.
   *
   * @param fn - The async function to execute.
   * @param options - Per-job scheduling options (priority, scheduleAt, etc.)
   * @returns A Promise that resolves with the function's return value.
   *
   * @example
   * const data = await limiter.schedule(() => fetch('/api/data').then(r => r.json()));
   */
  async schedule(fn, options) {
    if (this.stopped) throw new LimiterStoppedError(this.opts.id);
    await this.ensureInitialized();
    const jobId = (0, import_node_crypto.randomUUID)();
    const priority = options?.priority ?? this.opts.priority;
    const scheduleAt = options?.scheduleAt ?? 0;
    const maxRetries = options?.maxRetries ?? (this.opts.retryOnFailure ? this.opts.maxRetries : 0);
    const payload = JSON.stringify({
      id: jobId,
      priority,
      maxRetries,
      retryCount: 0,
      enqueuedAt: Date.now()
    });
    const score = scheduleAt === 0 ? 0 : scheduleAt + priority * 1e-3;
    return new Promise((resolve, reject) => {
      this.pool.pendingJobs.set(jobId, {
        fn,
        resolve,
        reject,
        maxRetries
      });
      this.pool.commandClient.bqEnqueue(
        KEYS.jobs(this.opts.id),
        KEYS.meta(this.opts.id),
        KEYS.notify,
        payload,
        String(score),
        String(this.opts.ttl),
        this.opts.id
      ).catch((err) => {
        this.pool.pendingJobs.delete(jobId);
        reject(err);
      });
    });
  }
  // ── Observability ──────────────────────────────────────────────────────────
  /**
   * Returns all queue statistics in a single atomic Redis read.
   */
  async getStats() {
    const raw = await this.pool.commandClient.bqStats(
      KEYS.jobs(this.opts.id),
      KEYS.running(this.opts.id),
      KEYS.reservoir(this.opts.id)
    );
    const reservoir = raw[2] === "-1" || raw[2] === null ? null : parseInt(raw[2], 10);
    return {
      queueSize: parseInt(raw[0] ?? "0", 10),
      runningCount: parseInt(raw[1] ?? "0", 10),
      reservoir
    };
  }
  /** Number of jobs waiting to be executed. */
  async getQueueSize() {
    return (await this.getStats()).queueSize;
  }
  /** Number of jobs currently executing. */
  async getRunningCount() {
    return (await this.getStats()).runningCount;
  }
  /**
   * Current reservoir token count.
   * Returns `null` if reservoir is not configured for this limiter.
   */
  async getReservoir() {
    return (await this.getStats()).reservoir;
  }
  // ── Queue management ───────────────────────────────────────────────────────
  /**
   * Update limiter settings. Changes are written to Redis and take effect on
   * the next dequeue attempt.
   */
  async updateSettings(patch) {
    const meta = {};
    if (patch.maxConcurrent !== void 0) meta["maxConcurrent"] = String(patch.maxConcurrent);
    if (patch.minTime !== void 0) meta["minTime"] = String(patch.minTime);
    if (patch.reservoir !== void 0) meta["reservoir"] = String(patch.reservoir);
    if (patch.reservoirRefreshInterval !== void 0) meta["reservoirRefreshInterval"] = String(patch.reservoirRefreshInterval);
    if (patch.reservoirRefreshAmount !== void 0) meta["reservoirRefreshAmount"] = String(patch.reservoirRefreshAmount);
    if (patch.ttl !== void 0) meta["ttl"] = String(patch.ttl);
    if (Object.keys(meta).length === 0) return;
    await this.pool.commandClient.hset(KEYS.meta(this.opts.id), meta);
  }
  /**
   * Wait until all currently queued and running jobs finish.
   * Polls Redis every 100ms.
   */
  async drain() {
    for (; ; ) {
      const stats = await this.getStats();
      if (stats.queueSize === 0 && stats.runningCount === 0) break;
      await new Promise((r) => setTimeout(r, 100));
    }
    this.emit("drained");
  }
  /**
   * Stop accepting new jobs. Does NOT drain existing jobs.
   * Call `drain()` first if you want to wait for pending jobs.
   */
  stop() {
    this.stopped = true;
  }
  // ── Internal ───────────────────────────────────────────────────────────────
  /**
   * Lazily synchronize queue meta to Redis and run crash recovery.
   * Only runs once per lifteime of the DistributedLimiter instance.
   */
  async ensureInitialized() {
    if (this.metaSynced && this.recoveryDone) return;
    if (!this.metaSynced) {
      await this.syncMeta();
      this.metaSynced = true;
    }
    if (!this.recoveryDone) {
      await this.pool.recoverQueue(this.opts.id, this.opts.ttl);
      this.recoveryDone = true;
    }
  }
  /** Write resolved options to the Redis HASH so Lua scripts can read them. */
  async syncMeta() {
    const meta = {
      maxConcurrent: String(this.opts.maxConcurrent),
      minTime: String(this.opts.minTime),
      reservoir: String(this.opts.reservoir),
      reservoirRefreshInterval: String(this.opts.reservoirRefreshInterval),
      reservoirRefreshAmount: String(this.opts.reservoirRefreshAmount),
      ttl: String(this.opts.ttl)
    };
    const client = this.pool.commandClient;
    await client.hset(KEYS.meta(this.opts.id), meta);
    await client.expire(KEYS.meta(this.opts.id), this.opts.ttl);
    if (this.opts.reservoir > 0) {
      await client.set(
        KEYS.reservoir(this.opts.id),
        String(this.opts.reservoir),
        "EX",
        this.opts.ttl,
        "NX"
        // only set if not exists
      );
    }
  }
};

// src/LimiterManager.ts
var DEFAULT_CONFIG = Object.freeze({
  maxConcurrent: 10,
  reservoir: -1,
  // -1 = unlimited (no token bucket)
  reservoirRefreshAmount: 10,
  reservoirRefreshInterval: 1e3,
  minTime: 0,
  timeout: 3e4,
  ttl: 600,
  // 10 minutes — idle queues auto-expire in Redis
  maxRetries: 3
});
var LimiterWrapper = class {
  _limiter;
  _timeout;
  constructor(limiter, timeout) {
    this._limiter = limiter;
    this._timeout = timeout;
  }
  schedule(optionsOrFn, fn) {
    let task;
    let jobOpts;
    if (typeof optionsOrFn === "function") {
      task = optionsOrFn;
    } else {
      if (!fn) throw new TypeError("BetterQueue: schedule() called with options but no function");
      task = fn;
      jobOpts = {
        ...optionsOrFn.priority !== void 0 && { priority: optionsOrFn.priority },
        ...optionsOrFn.scheduleAt !== void 0 && { scheduleAt: optionsOrFn.scheduleAt }
      };
    }
    const timeoutMs = this._timeout;
    const timedTask = timeoutMs > 0 ? () => Promise.race([
      task(),
      new Promise((_, reject) => {
        const handle = setTimeout(
          () => reject(new Error(`BetterQueue: job timed out after ${timeoutMs}ms`)),
          timeoutMs
        );
        if (handle.unref) handle.unref();
      })
    ]) : task;
    return this._limiter.schedule(timedTask, jobOpts);
  }
  // ── Bottleneck-compatible observability ──────────────────────────────────
  /**
   * Returns job counts in Bottleneck's format.
   * Useful for code that calls `limiter.counts()` for health checks.
   */
  async counts() {
    const stats = await this._limiter.getStats();
    return {
      RECEIVED: 0,
      // not tracked (no in-process queue)
      QUEUED: stats.queueSize,
      RUNNING: stats.runningCount,
      EXECUTING: stats.runningCount,
      DONE: 0
      // not tracked (Redis cleans up after completion)
    };
  }
  /** Direct access to the underlying DistributedLimiter if needed. */
  get inner() {
    return this._limiter;
  }
  /** Wait until this guild's queue is fully drained (all jobs done). */
  drain() {
    return this._limiter.drain();
  }
  /** Stop accepting new jobs for this limiter. */
  stop() {
    this._limiter.stop();
  }
};
var LimiterManager = class _LimiterManager {
  static _instance;
  /**
   * Guild×feature limiter cache.
   * Key: `${guildId}::${feature}`
   * Large max to avoid evicting active guild limiters.
   */
  _limiterCache;
  /**
   * Per-feature default configs registered via `registerFeatureConfig()`.
   */
  _featureConfigs;
  constructor() {
    this._limiterCache = new import_lru_cache.LRUCache({
      max: 1e4,
      // 10 min TTL — well beyond any in-flight Discord API call
      ttl: 1e3 * 60 * 10
      // DistributedLimiter has no timers — nothing to clean up on eviction
    });
    this._featureConfigs = new import_lru_cache.LRUCache({
      max: 200
    });
  }
  static getInstance() {
    if (!_LimiterManager._instance) {
      _LimiterManager._instance = new _LimiterManager();
    }
    return _LimiterManager._instance;
  }
  // ── Public API (matches original LimiterManager exactly) ─────────────────
  /**
   * Register the default rate-limit config for a feature.
   * Call once at startup (e.g. in the queue class constructor).
   *
   * @example
   * limiterManager.registerFeatureConfig('guild-member-add', {
   *   maxConcurrent: 1,
   *   reservoir: 10,
   *   reservoirRefreshAmount: 10,
   *   reservoirRefreshInterval: 10_000,
   *   minTime: 1_000,
   *   timeout: 3_600_000,
   * });
   */
  registerFeatureConfig(feature, config) {
    this._featureConfigs.set(feature, { ...DEFAULT_CONFIG, ...config });
  }
  /**
   * Get (or lazily create) a per-guild, per-feature rate limiter.
   *
   * @example
   * const limiter = limiterManager.getLimiter(guildId, 'guild-member-add');
   * await limiter.schedule({ id: `${guildId}-${memberId}` }, () => doWork());
   */
  getLimiter(guildId, feature) {
    const key = `${guildId}::${feature}`;
    const cached = this._limiterCache.get(key);
    if (cached) return cached;
    const cfg = this._featureConfigs.get(feature) ?? DEFAULT_CONFIG;
    const reservoirValue = cfg.reservoir !== void 0 && cfg.reservoir > 0 ? cfg.reservoir : -1;
    const opts = {
      id: `bq:${feature}:${guildId}`,
      maxConcurrent: cfg.maxConcurrent ?? DEFAULT_CONFIG.maxConcurrent,
      minTime: cfg.minTime ?? DEFAULT_CONFIG.minTime,
      reservoir: reservoirValue,
      reservoirRefreshAmount: cfg.reservoirRefreshAmount ?? DEFAULT_CONFIG.reservoirRefreshAmount,
      reservoirRefreshInterval: cfg.reservoirRefreshInterval ?? DEFAULT_CONFIG.reservoirRefreshInterval,
      ttl: cfg.ttl ?? DEFAULT_CONFIG.ttl,
      maxRetries: cfg.maxRetries ?? DEFAULT_CONFIG.maxRetries,
      retryOnFailure: (cfg.maxRetries ?? DEFAULT_CONFIG.maxRetries) > 0
    };
    const limiter = new DistributedLimiter(opts);
    const wrapper = new LimiterWrapper(limiter, cfg.timeout ?? DEFAULT_CONFIG.timeout);
    this._limiterCache.set(key, wrapper);
    return wrapper;
  }
  /**
   * Stop and remove all cached limiters.
   * Call on graceful shutdown.
   */
  async clearAll() {
    for (const wrapper of this._limiterCache.values()) {
      wrapper.stop();
    }
    this._limiterCache.clear();
    this._featureConfigs.clear();
  }
  /**
   * Cache utilization stats — matches original LimiterManager.getStats() format.
   */
  getStats() {
    const limiterMax = this._limiterCache.max ?? 1e4;
    const featureMax = this._featureConfigs.max ?? 200;
    const limiterUsage = this._limiterCache.size / limiterMax * 100;
    const featureUsage = this._featureConfigs.size / featureMax * 100;
    if (limiterUsage > 80) {
      console.warn(
        `[BetterQueue] Limiter cache usage is high: ${limiterUsage.toFixed(1)}% (${this._limiterCache.size}/${limiterMax})`
      );
    }
    return {
      limiterCache: {
        size: this._limiterCache.size,
        max: limiterMax,
        usage: `${limiterUsage.toFixed(1)}%`,
        ttl: this._limiterCache.ttl
      },
      featureConfigs: {
        size: this._featureConfigs.size,
        max: featureMax,
        usage: `${featureUsage.toFixed(1)}%`,
        ttl: this._featureConfigs.ttl
      }
    };
  }
  /** Health report — matches original LimiterManager.getCacheUsage() format. */
  getCacheUsage() {
    const stats = this.getStats();
    const usageNum = parseFloat(stats.limiterCache.usage);
    return {
      status: stats.limiterCache.usage,
      limiterCount: stats.limiterCache.size,
      maxLimiters: stats.limiterCache.max,
      isHealthy: usageNum < 80,
      recommendation: usageNum > 80 ? "Consider increasing cache size or optimizing feature usage" : "Cache usage is healthy"
    };
  }
  /**
   * Drain a specific guild+feature queue — wait until all pending jobs complete.
   */
  async drainGuild(guildId, feature) {
    const key = `${guildId}::${feature}`;
    const wrapper = this._limiterCache.get(key);
    if (wrapper) await wrapper.drain();
  }
};
var limiterManager = LimiterManager.getInstance();
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  BetterQueueError,
  DistributedLimiter,
  JobMaxRetriesError,
  KEYS,
  LimiterManager,
  LimiterStoppedError,
  LimiterWrapper,
  RedisCommandError,
  WorkerPool,
  WorkerPoolNotInitializedError,
  limiterManager
});
//# sourceMappingURL=index.js.map