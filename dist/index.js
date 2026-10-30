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
  isSentinelOptions: () => isSentinelOptions,
  limiterManager: () => limiterManager
});
module.exports = __toCommonJS(index_exports);

// src/WorkerPool.ts
var import_node_events = require("events");
var import_node_crypto = require("crypto");

// src/RedisClient.ts
var import_ioredis = __toESM(require("ioredis"));

// src/lua-scripts.ts
var LUA = {
  complete: `--[[
  complete.lua \u2014 Mark a job as complete, release its running slot, and
                 re-signal workers if more jobs exist in the queue.

  When the queue becomes fully empty (no pending, no running) all of its
  volatile keys are immediately deleted so Redis doesn't accumulate stale
  empty structures for inactive queues.  This is the "idle cleanup" path \u2014
  the queue meta key is left alive (so config survives cache-miss recreation)
  but with a short TTL refreshed on every touch.

  KEYS[1] = bq:{id}:running     (SET  \u2014 currently running job IDs)
  KEYS[2] = bq:{id}:inflight    (HASH \u2014 jobId \u2192 payload, crash recovery)
  KEYS[3] = bq:{id}:jobs        (ZSET \u2014 ready jobs)
  KEYS[4] = bq:{id}:meta        (HASH \u2014 queue config, TTL refreshed on activity)
  KEYS[5] = bq:{id}:lastRun     (STRING \u2014 timestamp of last job dequeue)
  KEYS[6] = bq:{id}:delayed     (ZSET \u2014 not-yet-due jobs)
  KEYS[7] = bq:global:wakeup    (ZSET \u2014 deferred worker wakeups)
  KEYS[8] = bq:pools            (ZSET \u2014 live pool registry)

  ARGV[1] = jobId               (string)
  ARGV[2] = queueId             (string \u2014 for re-notification)
  ARGV[3] = ttl                 (seconds \u2014 key expiry window)
  ARGV[4] = ownerPoolId         (pool id of the process that owns the queue)
  ARGV[5] = liveWindowMs        (how long a pool stays live without a beat)

  Returns: 1 if job was found and removed, 0 if job was not in running set
--]]

local runningKey  = KEYS[1]
local inflightKey = KEYS[2]
local jobsKey     = KEYS[3]
local metaKey     = KEYS[4]
local lastRunKey  = KEYS[5]
local delayedKey  = KEYS[6]
local wakeupKey   = KEYS[7]
local poolsKey    = KEYS[8]

local jobId   = ARGV[1]
local queueId = ARGV[2]
local ttl     = tonumber(ARGV[3])
local ownerPoolId = ARGV[4]
local liveWindowMs = tonumber(ARGV[5]) or 30000

local removed = redis.call('SREM', runningKey, jobId)
redis.call('HDEL', inflightKey, jobId)

if removed == 1 then
  local pending = redis.call('ZCARD', jobsKey) + redis.call('ZCARD', delayedKey)
  local running = redis.call('SCARD', runningKey)

  if pending > 0 then
    -- More jobs waiting \u2014 signal the owner and refresh activity TTLs.
    if ownerPoolId then
      redis.call('LPUSH', 'bq:workers:notify:' .. ownerPoolId, queueId)
    end

    -- A freed concurrency slot can unblock a job owned by a DIFFERENT
    -- process. Notify every other live pool as well, so a job waiting on a
    -- slot held elsewhere starts as soon as the slot is released instead of
    -- waiting for the next poll tick. (Mirrors the fan-out in enqueue.lua.)
    --
    -- Only queues that are actually concurrency-limited do this \u2014 an
    -- unlimited queue never has a waiter blocked on a slot, so it would
    -- just be noise.
    local maxConcurrent = tonumber(redis.call('HGET', metaKey, 'maxConcurrent') or '0')
    if maxConcurrent > 0 then
      local _time = redis.call('TIME')
      local now  = tonumber(_time[1]) * 1000 + math.floor(tonumber(_time[2]) / 1000)
      local livePools = redis.call(
        'ZRANGEBYSCORE', poolsKey,
        string.format('%.0f', now - liveWindowMs), '+inf'
      )
      for i = 1, #livePools do
        local pid = livePools[i]
        if (not ownerPoolId) or (pid ~= ownerPoolId) then
          redis.call('LPUSH', 'bq:workers:notify:' .. pid, queueId)
        end
      end
    end

    redis.call('EXPIRE', metaKey,  ttl)
    redis.call('EXPIRE', lastRunKey, ttl)
    redis.call('EXPIRE', runningKey, ttl)
    redis.call('EXPIRE', inflightKey, ttl)
    redis.call('EXPIRE', jobsKey, ttl)
    redis.call('EXPIRE', delayedKey, ttl)
  elseif running == 0 then
    -- Queue is completely idle \u2014 DELETE volatile runtime keys immediately.
    -- This frees Redis memory for inactive queues without waiting for TTL.
    -- meta key stays alive (short TTL) so getLimiter() cold-starts cheaply.
    redis.call('DEL', runningKey)
    redis.call('DEL', inflightKey)
    redis.call('DEL', jobsKey)
    redis.call('DEL', delayedKey)
    redis.call('DEL', lastRunKey)
    if ownerPoolId then
      redis.call('ZREM', wakeupKey, ownerPoolId .. '|' .. queueId)
    end
    redis.call('ZREM', wakeupKey, queueId)
    -- Meta survives with its current TTL \u2014 cheap to recreate, avoids syncMeta
    -- on every single event for queues that are somewhat active.
    redis.call('EXPIRE', metaKey, ttl)
  end
end

return removed
`,
  dequeue: `--[[
  dequeue.lua \u2014 Atomically fetch the next eligible job from a queue,
                enforcing maxConcurrent, minTime, and reservoir constraints.
                Performs lazy reservoir refresh/increase (no timers needed).

  OWNER-AWARE DEQUEUE
  -------------------
  A job's executable function is a JS closure and therefore lives in the
  process that scheduled it. A worker may therefore only run jobs that it
  owns. This script is called with the calling pool's id and scans the ready
  set for the first job owned by that pool (jobs without an \`owner\` field are
  claimable by anyone, for backwards compatibility).

  Rate-limit accounting (maxConcurrent / minTime / reservoir) is GLOBAL \u2014
  it is shared by every process working on the same queue id, which is what
  makes the limiter meaningful across a cluster.

  KEYS[1] = bq:{id}:jobs         (ZSET   \u2014 ready jobs, score = priority*1e12 + seq)
  KEYS[2] = bq:{id}:delayed      (ZSET   \u2014 not-yet-due jobs, score = due time ms)
  KEYS[3] = bq:{id}:running      (SET    \u2014 currently running job IDs)
  KEYS[4] = bq:{id}:inflight     (HASH   \u2014 jobId \u2192 payload, for crash recovery)
  KEYS[5] = bq:{id}:meta         (HASH   \u2014 queue configuration)
  KEYS[6] = bq:{id}:reservoir    (STRING \u2014 current token count)
  KEYS[7] = bq:{id}:lastRun      (STRING \u2014 ms timestamp of last dequeue)
  KEYS[8] = bq:{id}:lastRefresh  (STRING \u2014 ms timestamp of last reservoir refresh)
  KEYS[9] = bq:{id}:lastIncrease (STRING \u2014 ms timestamp of last reservoir increase)

  ARGV[1] = now                  (client clock, used only as a fallback)
  ARGV[2] = ttl                  (seconds, fallback when meta has none)
  ARGV[3] = ownerId              (pool id of the calling worker)
  ARGV[4] = scanWindow          (max entries to inspect per ZSET)

  Returns: table { status, payload, nextEligibleMs }
    status values:
      "ok"               \u2014 payload contains job JSON, nextEligibleMs = "0"
      "empty"            \u2014 nothing of ours is pending
      "blocked"          \u2014 our jobs exist but are further down the queue;
                            nextEligibleMs = when to look again
      "concurrent_full"  \u2014 every concurrency slot is taken (possibly by a
                            job owned by ANOTHER process) and we have a job
                            ready to run; nextEligibleMs = when to retry
      "rate_limited"     \u2014 minTime or reservoir not ready, nextEligibleMs = when to retry
--]]

local PRIORITY_SCALE = 1000000000000
local DEFAULT_SCAN_WINDOW = 256
-- How long to wait before looking again when every concurrency slot is
-- taken. We cannot know when the holder of the slot finishes, so this is a
-- short poll \u2014 the same cadence the "blocked" status already uses.
local CONCURRENCY_RETRY_MS = 50
-- How many entries of the running SET to inspect when working out whether
-- a concurrency slot is held by a job of this pool. Bounded so the check
-- stays O(1)-ish on large queues.
local MAX_OWNERSHIP_INSPECT = 32

local jobsKey         = KEYS[1]
local delayedKey      = KEYS[2]
local runningKey      = KEYS[3]
local inflightKey     = KEYS[4]
local metaKey         = KEYS[5]
local reservoirKey    = KEYS[6]
local lastRunKey      = KEYS[7]
local lastRefreshKey  = KEYS[8]
local lastIncreaseKey = KEYS[9]

local ownerId     = ARGV[3]
local scanWindow  = tonumber(ARGV[4]) or DEFAULT_SCAN_WINDOW
if scanWindow < 1 then scanWindow = 1 end

local function fmtScore(n)
  return string.format('%.0f', n)
end

-- Extract the owner of a job payload. Jobs without an owner field are
-- claimable by any worker (legacy payloads).
local function ownerOf(payload)
  return string.match(payload, '"owner"%s*:%s*"([^"]*)"')
end

-- Find the first entry in \`candidates\` owned by the calling pool.
local function firstOwned(candidates)
  for i = 1, #candidates do
    local p = candidates[i]
    local o = ownerOf(p)
    if (not o) or (o == ownerId) then
      return p
    end
  end
  return nil
end

-- Are ALL currently running jobs owned by the calling pool?
--
-- Used to tell two "the queue is full" situations apart:
--   * all ours \u2192 every one of those jobs will notify this pool when it
--                completes (complete.lua notifies the owner), so no polling
--                is needed. This keeps the single-process path exactly as
--                it was: no polling at all.
--   * not all ours \u2192 at least one slot is held by ANOTHER process. We
--                cannot know when it will be released, so we must poll.
--
-- The running SET holds at most \`maxConcurrent\` members and the script is
-- atomic, so the scan is exact. SSCAN is used instead of SMEMBERS so the
-- amount of data transferred stays bounded even for very large slots.
--
-- Any uncertainty (more entries than we are willing to inspect, or a
-- missing inflight record) is resolved as "not all ours" \u2014 polling is
-- always safe, it is only ever slightly more Redis traffic.
local function runningAllOurs(runningKey, inflightKey)
  local cursor = '0'
  local checked = 0
  repeat
    local res = redis.call('SSCAN', runningKey, cursor, 'COUNT', 16)
    cursor = res[1]
    local members = res[2]
    for i = 1, #members do
      checked = checked + 1
      if checked > MAX_OWNERSHIP_INSPECT then
        return false
      end
      local runningPayload = redis.call('HGET', inflightKey, members[i])
      if not runningPayload then
        return false
      end
      if ownerOf(runningPayload) ~= ownerId then
        return false
      end
    end
  until cursor == '0'
  return true
end

-- \u2500\u2500 Load queue meta \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
local metaArr = redis.call('HGETALL', metaKey)
local meta = {}
for i = 1, #metaArr, 2 do
  meta[metaArr[i]] = metaArr[i + 1]
end

local maxConcurrent       = tonumber(meta['maxConcurrent'] or '0')
local minTime             = tonumber(meta['minTime'] or '0')
local ttl                 = tonumber(meta['ttl'] or ARGV[2] or '86400')
local refreshInterval     = tonumber(meta['reservoirRefreshInterval'] or '0')
local refreshAmount       = tonumber(meta['reservoirRefreshAmount'] or '0')
local increaseInterval    = tonumber(meta['reservoirIncreaseInterval'] or '0')
local increaseAmount      = tonumber(meta['reservoirIncreaseAmount'] or '0')
local increaseMaximum     = tonumber(meta['reservoirIncreaseMaximum'] or '0')
local hasReservoir        = meta['reservoir'] ~= nil and meta['reservoir'] ~= '-1'

-- Use Redis server time as the authoritative clock. This prevents clock skew
-- between the Node.js client and the Redis server from causing jobs to be
-- missed (enqueued with server time, dequeued with client time).
local _time = redis.call('TIME')
local now  = tonumber(_time[1]) * 1000 + math.floor(tonumber(_time[2]) / 1000)

-- \u2500\u2500 Promote due delayed jobs into the ready set \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
-- A delayed job becomes runnable exactly when its due time passes. Promoting
-- it here keeps the whole transition atomic with the dequeue that runs it.
local dueJobs = redis.call('ZRANGEBYSCORE', delayedKey, '-inf', fmtScore(now))
if #dueJobs > 0 then
  for i = 1, #dueJobs do
    redis.call('ZREM', delayedKey, dueJobs[i])
    local prio = tonumber(string.match(dueJobs[i], '"priority"%s*:%s*(%d+)')) or 5
    if prio < 0 then prio = 0 end
    if prio > 99 then prio = 99 end
    local seq   = redis.call('HINCRBY', metaKey, 'seq', 1)
    local score = prio * PRIORITY_SCALE + seq
    redis.call('ZADD', jobsKey, fmtScore(score), dueJobs[i])
  end
  redis.call('EXPIRE', jobsKey, ttl)
  redis.call('EXPIRE', delayedKey, ttl)
end

-- \u2500\u2500 Lazy reservoir refresh + increase \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
-- Instead of a timer, we check on every dequeue attempt whether an interval
-- has elapsed and update the token bucket atomically.
--
--   refresh  \u2192 reservoir := reservoirRefreshAmount   (a RESET)
--   increase \u2192 reservoir := reservoir + reservoirIncreaseAmount (an ADD),
--              capped at reservoirIncreaseMaximum when configured.
local tokens = 0
if hasReservoir then
  local raw = redis.call('GET', reservoirKey)
  if raw then
    tokens = tonumber(raw)
  else
    -- The reservoir key expired (TTL) or was never created \u2014 re-seed it with
    -- the configured bucket size so the queue does not stay blocked forever.
    tokens = tonumber(meta['reservoir'] or '0')
    redis.call('SET', reservoirKey, tostring(tokens))
  end

  -- Refresh (RESET) and increase (ADD) are mutually exclusive: the refresh
  -- wins if both are configured, matching Bottleneck's behaviour.
  if refreshInterval > 0 and refreshAmount > 0 then
    local lastRefresh = tonumber(redis.call('GET', lastRefreshKey) or '0')
    if (now - lastRefresh) >= refreshInterval then
      tokens = refreshAmount
      redis.call('SET', reservoirKey, tostring(tokens))
      redis.call('SET', lastRefreshKey, tostring(now))
    end
  elseif increaseInterval > 0 and increaseAmount > 0 then
    local lastIncrease = tonumber(redis.call('GET', lastIncreaseKey) or '0')
    if (now - lastIncrease) >= increaseInterval then
      tokens = tokens + increaseAmount
      if increaseMaximum > 0 and tokens > increaseMaximum then
        tokens = increaseMaximum
      end
      redis.call('SET', reservoirKey, tostring(tokens))
      redis.call('SET', lastIncreaseKey, tostring(now))
    end
  end

  redis.call('EXPIRE', reservoirKey, ttl)
  if refreshInterval > 0 then redis.call('EXPIRE', lastRefreshKey, ttl) end
  if increaseInterval > 0 then redis.call('EXPIRE', lastIncreaseKey, ttl) end
end

-- \u2500\u2500 Compute the earliest time a job could run \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
local readyAt = now

if minTime > 0 then
  local lastRun = tonumber(redis.call('GET', lastRunKey) or '0')
  if (now - lastRun) < minTime then
    readyAt = lastRun + minTime
  end
end

if hasReservoir and tokens <= 0 then
  -- Calculate when the next token(s) arrive
  local nextToken = 0
  if refreshInterval > 0 and refreshAmount > 0 then
    local lastRefresh = tonumber(redis.call('GET', lastRefreshKey) or '0')
    nextToken = lastRefresh + refreshInterval
  elseif increaseInterval > 0 and increaseAmount > 0 then
    local lastIncrease = tonumber(redis.call('GET', lastIncreaseKey) or '0')
    nextToken = lastIncrease + increaseInterval
  end
  if nextToken == 0 then nextToken = now + 1000 end
  if nextToken > readyAt then readyAt = nextToken end
end

-- \u2500\u2500 Fetch the next ready job owned by this pool \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
local candidates = redis.call('ZRANGE', jobsKey, 0, scanWindow - 1)
local payload = firstOwned(candidates)

if payload then
  -- \u2500\u2500 Concurrency check \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
  -- We only reach this point with a job of OURS that we are about to run, so
  -- the check applies to the job this pool is actually taking \u2014 not to some
  -- unrelated job that happens to occupy a slot.
  if maxConcurrent > 0 then
    local running = tonumber(redis.call('SCARD', runningKey))
    if running >= maxConcurrent then
      -- Every slot is taken. Find out whether ALL of them are held by jobs
      -- of THIS pool:
      --
      --   * all ours  \u2192 when those jobs finish, complete.lua notifies this
      --                 pool directly, so no wakeup is needed (nextEligibleMs
      --                 = 0). This keeps the single-process path exactly as
      --                 it was: no polling at all.
      --   * not ours  \u2192 at least one slot belongs to ANOTHER process. We
      --                 cannot know when it will be released, so ask to be
      --                 woken up again shortly. Returning "no idea when to
      --                 retry" here is what used to strand the job in Redis
      --                 until an unrelated notification poked the queue.
      if runningAllOurs(runningKey, inflightKey) then
        return { 'concurrent_full', '', '0' }
      end
      return { 'concurrent_full', '', fmtScore(now + CONCURRENCY_RETRY_MS) }
    end
  end

  if readyAt > now then
    -- A ready job exists but minTime / reservoir says it cannot start yet.
    return { 'rate_limited', '', tostring(readyAt) }
  end

  -- \u2500\u2500 Consume reservoir token \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
  if hasReservoir then
    redis.call('DECR', reservoirKey)
  end

  -- \u2500\u2500 Move job to inflight tracking \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
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
end

-- \u2500\u2500 Nothing of ours is ready \u2014 check for our own delayed jobs \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
local delayedCandidates = redis.call('ZRANGE', delayedKey, 0, scanWindow - 1, 'WITHSCORES')
local myDelayed = nil
local myDelayedScore = nil
for i = 1, #delayedCandidates, 2 do
  local p = delayedCandidates[i]
  local o = ownerOf(p)
  if (not o) or (o == ownerId) then
    myDelayed = p
    myDelayedScore = tonumber(delayedCandidates[i + 1])
    break
  end
end

if myDelayed then
  local due = myDelayedScore
  if due < now then due = now end
  if due > readyAt then due = readyAt end
  return { 'empty', '', tostring(due) }
end

-- \u2500\u2500 Are our jobs further down the queue? \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
-- The ready set was full of other processes' jobs. If it is not exhausted
-- there may be one of ours further down \u2014 report "blocked" so the caller can
-- come back shortly.
if #candidates >= scanWindow then
  return { 'blocked', '', tostring(now + 50) }
end

return { 'empty', '', '0' }
`,
  enqueue: `--[[
  enqueue.lua \u2014 Atomically enqueue a job into a queue and signal workers.

  A job lands in one of two ZSETs:

    bq:{id}:jobs     READY   \u2014 may run as soon as the rate limits allow.
                                score = priority * 1e12 + seq
                                (lower score runs first \u2192 priority wins,
                                 arrival order breaks ties)
    bq:{id}:delayed  WAITING \u2014 not runnable yet (scheduleAt / retry backoff).
                                score = due time in ms

  The READY score is composed server-side so priority is applied on top of
  the Redis server clock (immune to client clock skew) and so a priority-1
  job always outranks a priority-9 job regardless of arrival order.

  NOTIFICATION MODEL
  ------------------
  A job's executable function is a JS closure that lives in the process that
  scheduled it \u2014 only that process can run it. Every WorkerPool therefore
  listens on its OWN channel (\`bq:workers:notify:<poolId>\`), and the job's
  owner id travels inside the payload. The notification is delivered
  straight to the owner's channel, so it can never be consumed by a
  different process.

  Live pools are additionally notified (fan-out) so that a queue shared by
  several processes can be drained by whichever process owns the next job.

  KEYS[1] = bq:{id}:jobs        (ZSET  \u2014 ready jobs)
  KEYS[2] = bq:{id}:delayed     (ZSET  \u2014 not-yet-due jobs)
  KEYS[3] = bq:{id}:meta        (HASH  \u2014 queue config, holds the \`seq\` counter)
  KEYS[4] = bq:global:wakeup    (ZSET  \u2014 deferred worker wakeups)
  KEYS[5] = bq:pools            (ZSET  \u2014 live pool registry)

  ARGV[1] = jobPayload          (JSON string, contains the \`owner\` pool id)
  ARGV[2] = scheduleAt          (absolute Unix ms, 0 = run now)
  ARGV[3] = delayMs             (relative delay in ms added to server time, 0 = none)
  ARGV[4] = priority            (integer; lower runs first)
  ARGV[5] = ttl                 (seconds for key expiry)
  ARGV[6] = queueId             (string \u2014 pushed to notify list)
  ARGV[7] = liveWindowMs        (how long a pool stays live without a beat)

  Returns: the due time (ms) used for the job.
--]]

local jobsKey    = KEYS[1]
local delayedKey = KEYS[2]
local metaKey    = KEYS[3]
local wakeupKey  = KEYS[4]
local poolsKey   = KEYS[5]

local payload    = ARGV[1]
local scheduleAt = tonumber(ARGV[2]) or 0
local delayMs    = tonumber(ARGV[3]) or 0
local priority   = tonumber(ARGV[4]) or 5
local ttl        = tonumber(ARGV[5])
local queueId    = ARGV[6]
local liveWindowMs = tonumber(ARGV[7]) or 30000

-- Priority occupies the high-order digits of the score so that a priority-1
-- job always outranks a priority-9 job. 1e12 leaves room for 1e12 arrivals
-- per priority band and keeps every composed score an exact integer
-- (Redis scores are IEEE doubles \u2192 integers are exact up to 2^53).
local PRIORITY_SCALE = 1000000000000

-- Use Redis server time as the authoritative clock. This prevents clock skew
-- between the Node.js client and the Redis server from causing jobs to be
-- missed or fired early.
local _time = redis.call('TIME')
local now  = tonumber(_time[1]) * 1000 + math.floor(tonumber(_time[2]) / 1000)

-- Resolve the due time
local due = now
if scheduleAt > 0 then
  due = scheduleAt
elseif delayMs > 0 then
  due = now + delayMs
end

-- Clamp priority so the composed score can never lose precision
if priority < 0 then priority = 0 end
if priority > 99 then priority = 99 end

local function fmtScore(n)
  return string.format('%.0f', n)
end

-- The pool that owns this job (and therefore holds its closure).
local ownerPoolId = string.match(payload, '"owner"%s*:%s*"([^"]*)"')

if due > now then
  -- Not runnable yet \u2014 park it in the delayed set, ordered by due time.
  redis.call('ZADD', delayedKey, fmtScore(due), payload)
  redis.call('EXPIRE', delayedKey, ttl)

  -- Schedule a wakeup so the job fires even if every worker is busy right now.
  -- "Keep the earliest wakeup": never push an existing wakeup further out.
  local member = ownerPoolId and (ownerPoolId .. '|' .. queueId) or queueId
  local current = redis.call('ZSCORE', wakeupKey, member)
  if (not current) or (tonumber(current) > due) then
    redis.call('ZADD', wakeupKey, fmtScore(due), member)
  end
else
  -- Runnable now \u2014 put it in the ready set ordered by priority, then arrival.
  local seq   = redis.call('HINCRBY', metaKey, 'seq', 1)
  local score = priority * PRIORITY_SCALE + seq
  redis.call('ZADD', jobsKey, fmtScore(score), payload)
  redis.call('EXPIRE', jobsKey, ttl)
end

-- Refresh meta TTL (touch active queue). The \`seq\` counter lives here, so the
-- TTL must outlive the job set \u2014 both are refreshed together.
redis.call('EXPIRE', metaKey, ttl)

-- \u2500\u2500 Notify the workers \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
-- The owner MUST be notified \u2014 that is the only process able to run the job.
if ownerPoolId then
  redis.call('LPUSH', 'bq:workers:notify:' .. ownerPoolId, queueId)
end

-- Fan out to the other live pools so that a queue shared by several
-- processes can be drained by whichever process owns the next job.
local livePools = redis.call(
  'ZRANGEBYSCORE', poolsKey,
  string.format('%.0f', now - liveWindowMs), '+inf'
)
for i = 1, #livePools do
  local pid = livePools[i]
  if (not ownerPoolId) or (pid ~= ownerPoolId) then
    redis.call('LPUSH', 'bq:workers:notify:' .. pid, queueId)
  end
end

return fmtScore(due)
`,
  poolHeartbeat: `--[[
  pool_heartbeat.lua \u2014 Register/refresh this WorkerPool's liveness in the
                       shared pool registry.

  The registry (bq:pools) is a ZSET whose score is the pool's last heartbeat
  timestamp (Redis server time). Pools that stop heartbeating fall out of the
  "live" window automatically, so the registry self-cleans \u2014 no GC needed.

  KEYS[1] = bq:pools            (ZSET \u2014 poolId \u2192 last heartbeat ms)

  ARGV[1] = poolId              (string)
  ARGV[2] = liveWindowMs        (how long a pool stays live without a beat)

  Returns: the server timestamp used for the heartbeat.
--]]

local poolsKey = KEYS[1]
local poolId  = ARGV[1]
local liveWindowMs = tonumber(ARGV[2]) or 30000

local _time = redis.call('TIME')
local now  = tonumber(_time[1]) * 1000 + math.floor(tonumber(_time[2]) / 1000)

redis.call('ZADD', poolsKey, string.format('%.0f', now), poolId)

-- Drop pools that stopped heartbeating (self-cleaning registry).
redis.call('ZREMRANGEBYSCORE', poolsKey, '-inf', string.format('%.0f', now - liveWindowMs))

return string.format('%.0f', now)
`,
  poolUnregister: `--[[
  pool_unregister.lua \u2014 Remove a WorkerPool from the live registry and clean
                        up its notification channel.

  Called when a pool shuts down gracefully, so other processes stop
  notifying it immediately instead of waiting for the liveness window to
  expire.

  KEYS[1] = bq:pools            (ZSET \u2014 poolId \u2192 last heartbeat ms)
  KEYS[2] = bq:workers:notify:<poolId>  (LIST \u2014 this pool's notify channel)

  ARGV[1] = poolId              (string)

  Returns: 1 if the pool was registered, 0 otherwise.
--]]

local poolsKey = KEYS[1]
local notifyKey = KEYS[2]
local poolId = ARGV[1]

local removed = redis.call('ZREM', poolsKey, poolId)
redis.call('DEL', notifyKey)

return removed
`,
  recover: `--[[
  recover.lua \u2014 On startup, re-enqueue any jobs from the inflight HASH that
                are older than a visibility timeout (likely orphaned by a crash).
                Designed to be called once at startup per known queue.

  KEYS[1] = bq:{id}:inflight    (HASH \u2014 jobId \u2192 payload)
  KEYS[2] = bq:{id}:running     (SET  \u2014 running job IDs)
  KEYS[3] = bq:{id}:jobs        (ZSET \u2014 ready jobs)
  KEYS[4] = bq:{id}:meta        (HASH \u2014 queue config, holds the \`seq\` counter)
  KEYS[5] = bq:global:wakeup    (ZSET \u2014 deferred worker wakeups)

  ARGV[1] = now                 (current time in ms)
  ARGV[2] = visibilityTimeout   (ms \u2014 jobs older than this are considered orphaned)
  ARGV[3] = queueId             (string)
  ARGV[4] = ttl                 (seconds for key expiry)
  ARGV[5] = ownerPoolId         (pool id of the process that owns the queue)

  Returns: number of jobs re-queued
--]]

local PRIORITY_SCALE = 1000000000000

local inflightKey = KEYS[1]
local runningKey  = KEYS[2]
local jobsKey     = KEYS[3]
local metaKey     = KEYS[4]
local wakeupKey   = KEYS[5]

local now               = tonumber(ARGV[1])
local visibilityTimeout = tonumber(ARGV[2])
local queueId           = ARGV[3]
local ttl               = tonumber(ARGV[4])
local ownerPoolId       = ARGV[5]

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
      -- Re-enqueue into the ready set using the same priority ordering
      -- as enqueue.lua so recovered jobs keep their place in the queue.
      local prio = tonumber(string.match(newPayload, '"priority"%s*:%s*(%d+)')) or 5
      if prio < 0 then prio = 0 end
      if prio > 99 then prio = 99 end
      local seq   = redis.call('HINCRBY', metaKey, 'seq', 1)
      local score = prio * PRIORITY_SCALE + seq
      redis.call('ZADD', jobsKey, string.format('%.0f', score), newPayload)
      redis.call('EXPIRE', jobsKey, ttl)
      requeued = requeued + 1
    end
    -- If maxRetries exhausted, job is dropped (no fn to call anyway after crash)
    --
    -- NOTE: the job's closure lived in the crashed process, so re-queueing it
    -- only preserves the queue's ordering/counters. It cannot be executed
    -- until the application re-submits the work.
  end
end

if requeued > 0 and ownerPoolId then
  -- Notify the owning pool that there is work to do.
  redis.call('LPUSH', 'bq:workers:notify:' .. ownerPoolId, queueId)
end

return requeued
`,
  stats: `--[[
  stats.lua \u2014 Atomically read queue statistics from Redis.

  Applies the same lazy reservoir maintenance as dequeue.lua, so the reported
  token count is accurate even when the queue is idle (no dequeue in flight).
  This keeps \`getReservoir()\` honest without adding a per-limiter timer.

  KEYS[1] = bq:{id}:jobs        (ZSET   \u2014 ready jobs)
  KEYS[2] = bq:{id}:running     (SET    \u2014 running job IDs)
  KEYS[3] = bq:{id}:reservoir   (STRING \u2014 current token count)
  KEYS[4] = bq:{id}:delayed     (ZSET   \u2014 not-yet-due jobs)
  KEYS[5] = bq:{id}:meta        (HASH   \u2014 queue configuration)
  KEYS[6] = bq:{id}:lastRefresh (STRING \u2014 ms timestamp of last reservoir refresh)
  KEYS[7] = bq:{id}:lastIncrease (STRING \u2014 ms timestamp of last reservoir increase)

  Returns: table { queueSize, runningCount, reservoir }
    queueSize counts ready + delayed (everything still pending)
    reservoir is "-1" if not configured
--]]

local jobsKey        = KEYS[1]
local runningKey     = KEYS[2]
local reservoirKey   = KEYS[3]
local delayedKey     = KEYS[4]
local metaKey        = KEYS[5]
local lastRefreshKey = KEYS[6]
local lastIncreaseKey = KEYS[7]

local queueSize    = redis.call('ZCARD', jobsKey) + redis.call('ZCARD', delayedKey)
local runningCount = redis.call('SCARD', runningKey)

-- \u2500\u2500 Load queue meta \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
local metaArr = redis.call('HGETALL', metaKey)
local meta = {}
for i = 1, #metaArr, 2 do
  meta[metaArr[i]] = metaArr[i + 1]
end

local hasReservoir = meta['reservoir'] ~= nil and meta['reservoir'] ~= '-1'
local reservoir = '-1'

if hasReservoir then
  local ttl                 = tonumber(meta['ttl'] or '86400')
  local refreshInterval     = tonumber(meta['reservoirRefreshInterval'] or '0')
  local refreshAmount       = tonumber(meta['reservoirRefreshAmount'] or '0')
  local increaseInterval    = tonumber(meta['reservoirIncreaseInterval'] or '0')
  local increaseAmount      = tonumber(meta['reservoirIncreaseAmount'] or '0')
  local increaseMaximum     = tonumber(meta['reservoirIncreaseMaximum'] or '0')

  local _time = redis.call('TIME')
  local now  = tonumber(_time[1]) * 1000 + math.floor(tonumber(_time[2]) / 1000)

  local raw = redis.call('GET', reservoirKey)
  local tokens
  if raw then
    tokens = tonumber(raw)
  else
    -- Never seeded (or expired) \u2014 start from the configured bucket size.
    tokens = tonumber(meta['reservoir'] or '0')
    redis.call('SET', reservoirKey, tostring(tokens))
  end

  -- Refresh (RESET) takes precedence over increase (ADD) when both are set.
  if refreshInterval > 0 and refreshAmount > 0 then
    local lastRefresh = tonumber(redis.call('GET', lastRefreshKey) or '0')
    if (now - lastRefresh) >= refreshInterval then
      tokens = refreshAmount
      redis.call('SET', reservoirKey, tostring(tokens))
      redis.call('SET', lastRefreshKey, tostring(now))
    end
  elseif increaseInterval > 0 and increaseAmount > 0 then
    local lastIncrease = tonumber(redis.call('GET', lastIncreaseKey) or '0')
    if (now - lastIncrease) >= increaseInterval then
      tokens = tokens + increaseAmount
      if increaseMaximum > 0 and tokens > increaseMaximum then
        tokens = increaseMaximum
      end
      redis.call('SET', reservoirKey, tostring(tokens))
      redis.call('SET', lastIncreaseKey, tostring(now))
    end
  end

  redis.call('EXPIRE', reservoirKey, ttl)
  if refreshInterval > 0 then redis.call('EXPIRE', lastRefreshKey, ttl) end
  if increaseInterval > 0 then redis.call('EXPIRE', lastIncreaseKey, ttl) end

  reservoir = tostring(tokens)
end

return {
  tostring(queueSize),
  tostring(runningCount),
  reservoir,
}
`,
  wakeupCheck: `--[[
  wakeup_check.lua \u2014 Drain the wakeup ZSET and signal workers for any
                     queues whose scheduled retry time has arrived.
                     Called by the single global ticker in WorkerPool.
                     This replaces ALL per-limiter timers.

  Wakeup members are "<poolId>|<queueId>". The poolId part identifies the
  process that owns the queue (and therefore holds the job closures), so the
  notification goes to that process' own channel.

  KEYS[1] = bq:global:wakeup    (ZSET \u2014 member \u2192 wakeup timestamp)
  KEYS[2] = bq:pools            (ZSET \u2014 live pool registry)

  ARGV[1] = now                 (ignored \u2014 the server clock is used instead)
  ARGV[2] = batchSize           (max queues to process per tick, e.g. "100")
  ARGV[3] = liveWindowMs        (how long a pool stays live without a beat)

  Returns: number of queues signalled
--]]

local wakeupKey = KEYS[1]
local poolsKey  = KEYS[2]

local batchSize = tonumber(ARGV[2]) or 100
local liveWindowMs = tonumber(ARGV[3]) or 30000

-- Use the Redis server clock as the authoritative "now".
--
-- Every wakeup score is written server-side (enqueue.lua, dequeue.lua), so
-- comparing against the server clock keeps the two in the same time base.
-- Using the client's clock here would let a client whose clock runs ahead
-- of the server wake up immediately after scheduling, turning the retry
-- interval into a hot loop.
local _time = redis.call('TIME')
local now   = tonumber(_time[1]) * 1000 + math.floor(tonumber(_time[2]) / 1000)
local nowStr = string.format('%.0f', now)

-- Fetch all wakeup entries whose time has come (up to batchSize)
local entries = redis.call('ZRANGEBYSCORE', wakeupKey, '-inf', nowStr, 'LIMIT', 0, batchSize)

if #entries == 0 then
  return 0
end

-- Which pools are alive right now?
local livePools = redis.call(
  'ZRANGEBYSCORE', poolsKey,
  string.format('%.0f', now - liveWindowMs), '+inf'
)

local function isLive(pid)
  for i = 1, #livePools do
    if livePools[i] == pid then return true end
  end
  return false
end

local signalled = 0

for _, member in ipairs(entries) do
  -- Remove only the entries we actually process \u2014 otherwise entries beyond
  -- the batch size would be silently dropped without ever being woken up.
  redis.call('ZREM', wakeupKey, member)

  local sep = string.find(member, '|', 1, true)
  local pid, queueId
  if sep then
    pid      = string.sub(member, 1, sep - 1)
    queueId  = string.sub(member, sep + 1)
  else
    pid      = nil
    queueId  = member
  end

  if pid then
    -- Notify the owning pool directly.
    redis.call('LPUSH', 'bq:workers:notify:' .. pid, queueId)
    signalled = signalled + 1
  else
    -- Legacy member without an owner \u2014 notify every live pool.
    for i = 1, #livePools do
      redis.call('LPUSH', 'bq:workers:notify:' .. livePools[i], queueId)
    end
    if #livePools > 0 then signalled = signalled + 1 end
  end
end

return signalled
`,
  wakeupSchedule: `--[[
  wakeup_schedule.lua \u2014 Atomically schedule a worker wakeup for a queue.

  The wakeup ZSET is a global registry of "queues that need to be looked at
  again later". A worker that could not run a job right now (rate limited,
  delayed, blocked by another process, or out of concurrency slots) parks
  the queue here so the global ticker wakes it up again at the right time.

  Entries are keyed by "<poolId>|<queueId>" so the ticker can notify the
  exact pool that asked for the wakeup \u2014 the pool that holds the job
  closures. One member per (pool, queue) pair means a queue can never
  accumulate more than a single pending wakeup.

  This script keeps the EARLIEST wakeup: an existing entry that fires
  sooner is never pushed back. Doing that atomically (inside Lua) avoids
  the race where two concurrent wakeup requests for the same queue read a
  stale score and one of them overwrites the other's earlier time \u2014 which
  could delay a job that was ready to run.

  KEYS[1] = bq:global:wakeup    (ZSET \u2014 member \u2192 wakeup timestamp in ms)

  ARGV[1] = member              ("<poolId>|<queueId>")
  ARGV[2] = atMs                (wakeup time in ms)

  Returns: 1 if the entry was created or moved earlier, 0 if an earlier
           entry was kept.
--]]

local wakeupKey = KEYS[1]
local member    = ARGV[1]
local atMs      = tonumber(ARGV[2])

if (not member) or (not atMs) then
  return 0
end

local current = redis.call('ZSCORE', wakeupKey, member)
if (not current) or (tonumber(current) > atMs) then
  redis.call('ZADD', wakeupKey, string.format('%.0f', atMs), member)
  return 1
end

return 0
`
};

// src/RedisClient.ts
function registerCommands(redis) {
  redis.defineCommand("bqEnqueue", {
    numberOfKeys: 5,
    // jobs, delayed, meta, wakeup, pools
    lua: LUA.enqueue
  });
  redis.defineCommand("bqDequeue", {
    numberOfKeys: 9,
    // jobs, delayed, running, inflight, meta, reservoir, lastRun, lastRefresh, lastIncrease
    lua: LUA.dequeue
  });
  redis.defineCommand("bqComplete", {
    numberOfKeys: 8,
    // running, inflight, jobs, meta, lastRun, delayed, wakeup, pools
    lua: LUA.complete
  });
  redis.defineCommand("bqWakeupSchedule", {
    numberOfKeys: 1,
    // wakeup
    lua: LUA.wakeupSchedule
  });
  redis.defineCommand("bqWakeupCheck", {
    numberOfKeys: 2,
    // wakeup, pools
    lua: LUA.wakeupCheck
  });
  redis.defineCommand("bqRecover", {
    numberOfKeys: 5,
    // inflight, running, jobs, meta, wakeup
    lua: LUA.recover
  });
  redis.defineCommand("bqStats", {
    numberOfKeys: 7,
    lua: LUA.stats
  });
  redis.defineCommand("bqPoolHeartbeat", {
    numberOfKeys: 1,
    // pools
    lua: LUA.poolHeartbeat
  });
  redis.defineCommand("bqPoolUnregister", {
    numberOfKeys: 2,
    // pools, notifyPool
    lua: LUA.poolUnregister
  });
}
function toIoredisOptions(connection) {
  if (!isSentinelOptions(connection)) return connection;
  const { sentinels, name, sentinelPassword, sentinelUsername, password, username, tls, redisOptions } = connection;
  return {
    sentinels,
    name,
    ...sentinelPassword ? { sentinelPassword } : {},
    ...sentinelUsername ? { sentinelUsername } : {},
    ...password ? { password } : {},
    ...username ? { username } : {},
    ...tls ? { tls } : {},
    ...redisOptions ?? {}
  };
}
function isSentinelOptions(connection) {
  return typeof connection === "object" && connection !== null && Array.isArray(connection.sentinels) && connection.sentinels.length > 0 && typeof connection.name === "string" && connection.name.length > 0;
}
function createRedisClient(options) {
  const client = new import_ioredis.default({
    enableOfflineQueue: true,
    lazyConnect: false,
    maxRetriesPerRequest: 3,
    ...toIoredisOptions(options)
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
    ...toIoredisOptions(options)
  });
}

// src/constants.ts
var KEYS = {
  /** ZSET — ready jobs, score = priority * 1e12 + arrival seq */
  jobs: (id) => `bq:${id}:jobs`,
  /** ZSET — jobs not yet due (scheduleAt / retry backoff), score = due time (ms) */
  delayed: (id) => `bq:${id}:delayed`,
  /** SET — job IDs of currently executing jobs */
  running: (id) => `bq:${id}:running`,
  /** HASH — jobId → full payload, for crash recovery */
  inflight: (id) => `bq:${id}:inflight`,
  /** HASH — queue configuration (maxConcurrent, minTime, etc.) + arrival counter */
  meta: (id) => `bq:${id}:meta`,
  /** STRING (int) — current token count for reservoir */
  reservoir: (id) => `bq:${id}:reservoir`,
  /** STRING (ms) — timestamp of last job dequeue */
  lastRun: (id) => `bq:${id}:lastRun`,
  /** STRING (ms) — timestamp of last reservoir refresh */
  lastRefresh: (id) => `bq:${id}:lastRefresh`,
  /** STRING (ms) — timestamp of last reservoir increase */
  lastIncrease: (id) => `bq:${id}:lastIncrease`,
  /**
   * LIST — per-pool notification channel.
   *
   * Each WorkerPool listens on its OWN channel so that a notification for
   * pool A is never consumed by pool B. Without this, a second process would
   * steal the first process's notification and its job would never run.
   */
  notifyPool: (poolId) => `bq:workers:notify:${poolId}`,
  /** ZSET — scheduled worker wakeups, score = wakeup time (ms) */
  wakeup: "bq:global:wakeup",
  /** ZSET — live WorkerPool registry, score = last heartbeat (ms) */
  pools: "bq:pools"
};
var POOL_LIVE_WINDOW_MS = 3e4;
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
    const detail = cause instanceof Error ? cause.message : String(cause);
    super(`Job ${jobId} failed after ${attempts} attempt(s): ${detail}`);
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
var OrphanedJobError = class extends BetterQueueError {
  constructor(jobId) {
    super(
      `Job ${jobId} was evicted from the pending-jobs map without executing. The Redis notification was likely lost. The job may still be enqueued in Redis and will be retried when crash-recovery runs.`
    );
    this.jobId = jobId;
    this.name = "OrphanedJobError";
  }
  jobId;
};

// src/WorkerPool.ts
var DEFAULT_PENDING_JOB_TIMEOUT_MS = 3e5;
var SWEEPER_INTERVAL_MS = 3e4;
var DEQUEUE_SCAN_WINDOW = 256;
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
          KEYS.notifyPool(this.pool.poolId),
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
      if ((status === "rate_limited" || status === "empty" || status === "blocked" || status === "concurrent_full") && nextEligibleMs > 0) {
        await this.scheduleWakeup(queueId, nextEligibleMs);
      }
      break;
    }
  }
  async tryDequeue(queueId) {
    try {
      const raw = await this.pool.commandClient.bqDequeue(
        KEYS.jobs(queueId),
        KEYS.delayed(queueId),
        KEYS.running(queueId),
        KEYS.inflight(queueId),
        KEYS.meta(queueId),
        KEYS.reservoir(queueId),
        KEYS.lastRun(queueId),
        KEYS.lastRefresh(queueId),
        KEYS.lastIncrease(queueId),
        String(Date.now()),
        String(this.pool.defaultTtl),
        this.pool.poolId,
        String(DEQUEUE_SCAN_WINDOW)
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
    this.pool.dispatchExecuting(queueId, job.id);
    try {
      const value = await pending.fn();
      this.pool.pendingJobs.delete(job.id);
      await this.completeJob(queueId, job.id);
      pending.resolve(value);
      this.pool.dispatchDone(queueId, job.id);
    } catch (err) {
      await this.handleJobError(queueId, job, pending, err);
    }
  }
  /**
   * Handle a failed job: retry with exponential backoff, or reject the
   * caller's promise once the retry budget is exhausted.
   */
  async handleJobError(queueId, job, pending, err) {
    const attempt = job.retryCount + 1;
    if (job.retryCount < job.maxRetries) {
      const delayMs = Math.min(1e3 * 2 ** job.retryCount, 3e4);
      const retryPayload = { ...job, retryCount: attempt };
      this.pool.dispatchRetry(queueId, job.id, attempt);
      try {
        await this.pool.commandClient.bqEnqueue(
          KEYS.jobs(queueId),
          KEYS.delayed(queueId),
          KEYS.meta(queueId),
          KEYS.wakeup,
          KEYS.pools,
          JSON.stringify(retryPayload),
          "0",
          // scheduleAt — absolute, not used for retries
          String(delayMs),
          String(job.priority),
          String(this.pool.defaultTtl),
          queueId,
          String(POOL_LIVE_WINDOW_MS)
        );
      } catch {
        this.pool.pendingJobs.delete(job.id);
        const retryErr = new JobMaxRetriesError(job.id, attempt, err);
        pending.reject(retryErr);
        this.pool.dispatchFailed(queueId, job.id, retryErr);
        return;
      }
      await this.completeJob(queueId, job.id);
    } else {
      this.pool.pendingJobs.delete(job.id);
      await this.completeJob(queueId, job.id);
      const finalErr = new JobMaxRetriesError(job.id, attempt, err);
      pending.reject(finalErr);
      this.pool.dispatchFailed(queueId, job.id, finalErr);
    }
  }
  async completeJob(queueId, jobId) {
    try {
      await this.pool.commandClient.bqComplete(
        KEYS.running(queueId),
        KEYS.inflight(queueId),
        KEYS.jobs(queueId),
        KEYS.meta(queueId),
        // for TTL refresh + idle cleanup signal
        KEYS.lastRun(queueId),
        // deleted immediately when queue goes idle
        KEYS.delayed(queueId),
        KEYS.wakeup,
        KEYS.pools,
        jobId,
        queueId,
        String(this.pool.defaultTtl),
        this.pool.poolId,
        String(POOL_LIVE_WINDOW_MS)
      );
    } catch {
    }
  }
  async scheduleWakeup(queueId, atMs) {
    try {
      await this.pool.commandClient.bqWakeupSchedule(
        KEYS.wakeup,
        `${this.pool.poolId}|${queueId}`,
        String(atMs)
      );
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
  /**
   * Per-queue event subscribers.
   *
   * A `DistributedLimiter` subscribes to the pool's job events for its own
   * queue id. Keeping them in a map (instead of adding a listener per
   * limiter to this shared emitter) means the emitter's listener count stays
   * constant no matter how many limiters exist.
   */
  queueSubscribers = /* @__PURE__ */ new Map();
  defaultTtl;
  /**
   * Unique id of this WorkerPool instance (one per process).
   *
   * Jobs are stamped with the id of the pool that scheduled them. Because a
   * job's executable function is a JS closure living in the scheduling
   * process, only that process may run it — so the id is what makes
   * cross-process coordination safe.
   */
  poolId;
  workers;
  redisOptions;
  tickIntervalMs;
  visibilityTimeoutMs;
  pendingJobTimeoutMs;
  tickerHandle = null;
  sweeperHandle = null;
  stopped = false;
  constructor(options) {
    super();
    const workerCount = options.workers ?? DEFAULT_WORKER_COUNT;
    this.tickIntervalMs = options.tickIntervalMs ?? DEFAULT_TICK_INTERVAL_MS;
    this.visibilityTimeoutMs = options.visibilityTimeoutMs ?? DEFAULT_VISIBILITY_TIMEOUT_MS;
    this.pendingJobTimeoutMs = options.pendingJobTimeoutMs ?? DEFAULT_PENDING_JOB_TIMEOUT_MS;
    this.defaultTtl = 86400;
    this.redisOptions = options.redis;
    this.poolId = `${process.pid}-${(0, import_node_crypto.randomUUID)().slice(0, 8)}`;
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
  /**
   * Subscribe to job events for a single queue id.
   *
   * Returns an unsubscribe function.
   */
  subscribeQueue(queueId, handlers) {
    let entry = this.queueSubscribers.get(queueId);
    if (!entry) {
      entry = { executing: /* @__PURE__ */ new Set(), done: /* @__PURE__ */ new Set(), failed: /* @__PURE__ */ new Set(), retry: /* @__PURE__ */ new Set() };
      this.queueSubscribers.set(queueId, entry);
    }
    if (handlers.executing) entry.executing.add(handlers.executing);
    if (handlers.done) entry.done.add(handlers.done);
    if (handlers.failed) entry.failed.add(handlers.failed);
    if (handlers.retry) entry.retry.add(handlers.retry);
    return () => {
      const current = this.queueSubscribers.get(queueId);
      if (!current) return;
      if (handlers.executing) current.executing.delete(handlers.executing);
      if (handlers.done) current.done.delete(handlers.done);
      if (handlers.failed) current.failed.delete(handlers.failed);
      if (handlers.retry) current.retry.delete(handlers.retry);
      if (current.executing.size === 0 && current.done.size === 0 && current.failed.size === 0 && current.retry.size === 0) {
        this.queueSubscribers.delete(queueId);
      }
    };
  }
  /** Notify the subscribers of a queue that a job started executing. */
  dispatchExecuting(queueId, jobId) {
    const entry = this.queueSubscribers.get(queueId);
    if (!entry) return;
    for (const fn of entry.executing) {
      try {
        fn(jobId);
      } catch {
      }
    }
  }
  /** Notify the subscribers of a queue that a job completed. */
  dispatchDone(queueId, jobId) {
    const entry = this.queueSubscribers.get(queueId);
    if (!entry) return;
    for (const fn of entry.done) {
      try {
        fn(jobId);
      } catch {
      }
    }
  }
  /** Notify the subscribers of a queue that a job failed. */
  dispatchFailed(queueId, jobId, error) {
    const entry = this.queueSubscribers.get(queueId);
    if (!entry) return;
    for (const fn of entry.failed) {
      try {
        fn(jobId, error);
      } catch {
      }
    }
  }
  /** Notify the subscribers of a queue that a job is being retried. */
  dispatchRetry(queueId, jobId, attempt) {
    const entry = this.queueSubscribers.get(queueId);
    if (!entry) return;
    for (const fn of entry.retry) {
      try {
        fn(jobId, attempt);
      } catch {
      }
    }
  }
  async start() {
    for (const worker of this.workers) {
      worker.start();
    }
    await this.heartbeat();
    this.startTicker();
    if (this.pendingJobTimeoutMs > 0) this.startSweeper();
    const shutdown = () => {
      void this.stop();
    };
    process.once("SIGINT", shutdown);
    process.once("SIGTERM", shutdown);
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
    if (this.sweeperHandle) {
      clearInterval(this.sweeperHandle);
      this.sweeperHandle = null;
    }
    for (const [jobId, pending] of this.pendingJobs) {
      pending.reject(new OrphanedJobError(jobId));
    }
    this.pendingJobs.clear();
    await Promise.all(this.workers.map((w) => w.stop()));
    try {
      await this.commandClient.bqPoolUnregister(
        KEYS.pools,
        KEYS.notifyPool(this.poolId),
        this.poolId
      );
    } catch {
    }
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
        KEYS.meta(queueId),
        KEYS.wakeup,
        String(Date.now()),
        String(this.visibilityTimeoutMs),
        queueId,
        String(ttl),
        this.poolId
      );
    } catch {
      return 0;
    }
  }
  startTicker() {
    const tick = () => {
      void this.commandClient.bqWakeupCheck(
        KEYS.wakeup,
        KEYS.pools,
        String(Date.now()),
        String(WAKEUP_BATCH_SIZE),
        String(POOL_LIVE_WINDOW_MS)
      ).catch(() => {
      });
      void this.heartbeat();
    };
    this.tickerHandle = setInterval(tick, this.tickIntervalMs);
    this.tickerHandle.unref();
  }
  /** Refresh this pool's liveness heartbeat in Redis. */
  async heartbeat() {
    try {
      await this.commandClient.bqPoolHeartbeat(
        KEYS.pools,
        this.poolId,
        String(POOL_LIVE_WINDOW_MS)
      );
    } catch {
    }
  }
  /**
   * Periodically scans `pendingJobs` and rejects any entry that has been
   * waiting longer than `pendingJobTimeoutMs`. This catches jobs whose Redis
   * BRPOP notification was lost (e.g. Redis restart, network blip) and would
   * otherwise hold their closures in memory forever.
   */
  startSweeper() {
    const sweep = () => {
      const cutoff = Date.now() - this.pendingJobTimeoutMs;
      for (const [jobId, pending] of this.pendingJobs) {
        if (pending.enqueuedAt < cutoff) {
          this.pendingJobs.delete(jobId);
          pending.reject(new OrphanedJobError(jobId));
          this.dispatchFailed(pending.queueId, jobId, new OrphanedJobError(jobId));
        }
      }
    };
    this.sweeperHandle = setInterval(sweep, SWEEPER_INTERVAL_MS);
    this.sweeperHandle.unref();
  }
};

// src/LimiterManager.ts
var import_lru_cache = require("lru-cache");

// src/DistributedLimiter.ts
var import_node_crypto2 = require("crypto");
var import_node_events2 = require("events");
var DistributedLimiter = class extends import_node_events2.EventEmitter {
  opts;
  pool;
  stopped = false;
  metaSynced = false;
  recoveryDone = false;
  /** Detaches this limiter's listeners from the shared WorkerPool emitter. */
  detachPoolListeners;
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
      reservoirIncreaseInterval: options.reservoirIncreaseInterval ?? 0,
      reservoirIncreaseAmount: options.reservoirIncreaseAmount ?? 0,
      reservoirIncreaseMaximum: options.reservoirIncreaseMaximum ?? 0,
      priority: options.priority ?? DEFAULT_PRIORITY,
      ttl: options.ttl ?? DEFAULT_TTL_SECONDS,
      retryOnFailure: options.retryOnFailure ?? true,
      maxRetries: options.maxRetries ?? DEFAULT_MAX_RETRIES
    };
    this.detachPoolListeners = this.pool.subscribeQueue(this.opts.id, {
      executing: (jobId) => {
        this.emit("executing", jobId);
      },
      done: (jobId) => {
        this.emit("done", jobId);
      },
      failed: (jobId, error) => {
        this.emit("failed", jobId, error);
      },
      retry: (jobId, attempt) => {
        this.emit("retry", jobId, attempt);
      }
    });
  }
  /** The unique queue identifier. */
  get id() {
    return this.opts.id;
  }
  async schedule(fnOrOptions, optionsOrFn) {
    if (this.stopped) throw new LimiterStoppedError(this.opts.id);
    let fn;
    let options;
    if (typeof fnOrOptions === "function") {
      fn = fnOrOptions;
      options = typeof optionsOrFn === "function" ? void 0 : optionsOrFn;
    } else {
      if (typeof optionsOrFn !== "function") {
        throw new TypeError("BetterQueue: schedule() called with options but no function");
      }
      fn = optionsOrFn;
      options = fnOrOptions;
    }
    await this.ensureInitialized();
    const jobId = (0, import_node_crypto2.randomUUID)();
    const priority = options?.priority ?? this.opts.priority;
    const scheduleAt = options?.scheduleAt ?? 0;
    const maxRetries = options?.maxRetries ?? (this.opts.retryOnFailure ? this.opts.maxRetries : 0);
    const payload = JSON.stringify({
      id: jobId,
      priority,
      maxRetries,
      retryCount: 0,
      enqueuedAt: Date.now(),
      owner: this.pool.poolId
    });
    return new Promise((resolve, reject) => {
      this.pool.pendingJobs.set(jobId, {
        fn,
        resolve,
        reject,
        maxRetries,
        enqueuedAt: Date.now(),
        queueId: this.opts.id
      });
      this.pool.commandClient.bqEnqueue(
        KEYS.jobs(this.opts.id),
        KEYS.delayed(this.opts.id),
        KEYS.meta(this.opts.id),
        KEYS.wakeup,
        KEYS.pools,
        payload,
        String(scheduleAt),
        "0",
        // relative delay — not used for user-scheduled jobs
        String(priority),
        String(this.opts.ttl),
        this.opts.id,
        String(POOL_LIVE_WINDOW_MS)
      ).catch((err) => {
        this.pool.pendingJobs.delete(jobId);
        reject(err);
      });
    });
  }
  // ── Observability ──────────────────────────────────────────────────────────
  /**
   * Returns all queue statistics in a single atomic Redis read.
   *
   * Initializes the queue on first use (meta + reservoir) so that stats are
   * meaningful even before the first job is scheduled.
   */
  async getStats() {
    await this.ensureInitialized();
    const raw = await this.pool.commandClient.bqStats(
      KEYS.jobs(this.opts.id),
      KEYS.running(this.opts.id),
      KEYS.reservoir(this.opts.id),
      KEYS.delayed(this.opts.id),
      KEYS.meta(this.opts.id),
      KEYS.lastRefresh(this.opts.id),
      KEYS.lastIncrease(this.opts.id)
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
    if (patch.reservoirIncreaseInterval !== void 0) meta["reservoirIncreaseInterval"] = String(patch.reservoirIncreaseInterval);
    if (patch.reservoirIncreaseAmount !== void 0) meta["reservoirIncreaseAmount"] = String(patch.reservoirIncreaseAmount);
    if (patch.reservoirIncreaseMaximum !== void 0) meta["reservoirIncreaseMaximum"] = String(patch.reservoirIncreaseMaximum);
    if (patch.ttl !== void 0) meta["ttl"] = String(patch.ttl);
    if (Object.keys(meta).length === 0) return;
    await this.pool.commandClient.hset(KEYS.meta(this.opts.id), meta);
  }
  /**
   * Wait until all currently queued and running jobs finish.
   * Polls Redis every 100ms.
   *
   * @param timeoutMs - If provided, rejects with a `DrainTimeoutError` after
   *   this many milliseconds so `drain()` can never block forever when a job
   *   notification is lost. Defaults to no timeout.
   */
  async drain(timeoutMs) {
    const deadline = timeoutMs !== void 0 ? Date.now() + timeoutMs : Infinity;
    for (; ; ) {
      if (Date.now() >= deadline) {
        throw new Error(
          `drain() timed out after ${timeoutMs}ms \u2014 queue "${this.opts.id}" may have orphaned jobs.`
        );
      }
      const stats = await this.getStats();
      if (stats.queueSize === 0 && stats.runningCount === 0) break;
      await new Promise((r) => setTimeout(r, 100));
    }
    this.emit("drained");
  }
  /**
   * Stop accepting new jobs. Does NOT drain existing jobs.
   * Call `drain()` first if you want to wait for pending jobs.
   * Removes all event listeners to free any captured closures.
   */
  stop() {
    this.stopped = true;
    this.detachPoolListeners?.();
    this.removeAllListeners();
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
      reservoirIncreaseInterval: String(this.opts.reservoirIncreaseInterval),
      reservoirIncreaseAmount: String(this.opts.reservoirIncreaseAmount),
      reservoirIncreaseMaximum: String(this.opts.reservoirIncreaseMaximum),
      ttl: String(this.opts.ttl)
    };
    const client = this.pool.commandClient;
    await client.hset(KEYS.meta(this.opts.id), meta);
    await client.expire(KEYS.meta(this.opts.id), this.opts.ttl);
    if (this.opts.reservoir >= 0) {
      const seeded = await client.set(
        KEYS.reservoir(this.opts.id),
        String(this.opts.reservoir),
        "EX",
        this.opts.ttl,
        "NX"
        // only set if not exists
      );
      if (seeded === "OK") {
        const now = String(Date.now());
        await client.set(KEYS.lastRefresh(this.opts.id), now, "EX", this.opts.ttl);
        await client.set(KEYS.lastIncrease(this.opts.id), now, "EX", this.opts.ttl);
      }
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
  reservoirIncreaseInterval: 0,
  // 0 = disabled (token-bucket "add" mode)
  reservoirIncreaseAmount: 0,
  reservoirIncreaseMaximum: 0,
  // 0 = unbounded
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
  /** Wait until this queue is fully drained (all jobs done). */
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
   * Scope×feature limiter cache.
   * Key: `${scopeId}::${feature}`
   * Large max to avoid evicting active limiters that are still in use.
   */
  _limiterCache;
  /**
   * Per-feature default configs registered via `registerFeatureConfig()`.
   */
  _featureConfigs;
  constructor() {
    this._limiterCache = new import_lru_cache.LRUCache({
      max: 1e4,
      // 10 min TTL — well beyond any in-flight job
      ttl: 1e3 * 60 * 10,
      ttlAutopurge: true,
      // CRITICAL: call stop() on every eviction (TTL expiry, LRU displacement, or
      // manual delete). This ensures removeAllListeners() always fires, preventing
      // the silent event-listener leak on LRU cache churn.
      dispose: (wrapper) => {
        wrapper.stop();
      }
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
   * limiterManager.registerFeatureConfig('member-add', {
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
   * Get (or lazily create) a per-scope, per-feature rate limiter.
   *
   * @example
   * const limiter = limiterManager.getLimiter(scopeId, 'member-add');
   * await limiter.schedule({ id: `${scopeId}-${itemId}` }, () => doWork());
   */
  getLimiter(scopeId, feature) {
    const key = `${scopeId}::${feature}`;
    const cached = this._limiterCache.get(key);
    if (cached) return cached;
    const cfg = this._featureConfigs.get(feature) ?? DEFAULT_CONFIG;
    const reservoirValue = cfg.reservoir !== void 0 && cfg.reservoir > 0 ? cfg.reservoir : -1;
    const opts = {
      id: `bq:${feature}:${scopeId}`,
      maxConcurrent: cfg.maxConcurrent ?? DEFAULT_CONFIG.maxConcurrent,
      minTime: cfg.minTime ?? DEFAULT_CONFIG.minTime,
      reservoir: reservoirValue,
      reservoirRefreshAmount: cfg.reservoirRefreshAmount ?? DEFAULT_CONFIG.reservoirRefreshAmount,
      reservoirRefreshInterval: cfg.reservoirRefreshInterval ?? DEFAULT_CONFIG.reservoirRefreshInterval,
      reservoirIncreaseInterval: cfg.reservoirIncreaseInterval ?? DEFAULT_CONFIG.reservoirIncreaseInterval,
      reservoirIncreaseAmount: cfg.reservoirIncreaseAmount ?? DEFAULT_CONFIG.reservoirIncreaseAmount,
      reservoirIncreaseMaximum: cfg.reservoirIncreaseMaximum ?? DEFAULT_CONFIG.reservoirIncreaseMaximum,
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
   * Remove a single scope+feature limiter from the cache.
   * The limiter is stopped immediately, freeing its event listeners.
   *
   * Previously a no-op because the method did not exist — NightmodeQueue was
   * already trying to call this. Now it works.
   *
   * @example
   * // On scope deletion or per-feature cleanup:
   * limiterManager.removeLimiter(scopeId, 'nightmode-scheduler');
   */
  removeLimiter(scopeId, feature) {
    const key = `${scopeId}::${feature}`;
    this._limiterCache.delete(key);
  }
  /**
   * Remove ALL limiters for a specific scope and stop them immediately.
   * Call this when a scope goes away to free memory — its limiters would
   * otherwise sit in the LRU for up to 10 minutes.
   *
   * @example
   * client.on('scopeDeleted', (scope) => {
   *   limiterManager.removeScopeLimiters(scope.id);
   * });
   */
  removeScopeLimiters(scopeId) {
    const prefix = `${scopeId}::`;
    const toDelete = [];
    for (const key of this._limiterCache.keys()) {
      if (key.startsWith(prefix)) toDelete.push(key);
    }
    for (const key of toDelete) {
      this._limiterCache.delete(key);
    }
  }
  /**
   * Stop and remove all cached limiters.
   * Call on graceful shutdown.
   */
  async clearAll() {
    this._limiterCache.clear();
    this._featureConfigs.clear();
  }
  /**
   * Cache utilization stats — matches original LimiterManager.getStats() format.
   * Now also exposes `pendingJobsSize` — the #1 indicator of a job-queue backlog.
   * If this number climbs continuously, jobs are queuing faster than workers execute.
   */
  getStats() {
    const limiterMax = this._limiterCache.max ?? 1e4;
    const featureMax = this._featureConfigs.max ?? 200;
    const limiterUsage = this._limiterCache.size / limiterMax * 100;
    const featureUsage = this._featureConfigs.size / featureMax * 100;
    let pendingJobsSize = 0;
    try {
      pendingJobsSize = WorkerPool.getInstance().pendingJobs.size;
    } catch {
    }
    if (limiterUsage > 80) {
      console.warn(
        `[BetterQueue] Limiter cache usage is high: ${limiterUsage.toFixed(1)}% (${this._limiterCache.size}/${limiterMax})`
      );
    }
    if (pendingJobsSize > 500) {
      console.warn(
        `[BetterQueue] pendingJobs backlog is high: ${pendingJobsSize} entries. Jobs may be queuing faster than workers can drain them.`
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
      },
      // Primary leak indicator — monitor this value over time
      pendingJobsSize
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
   * Drain a specific scope+feature queue — wait until all pending jobs complete.
   */
  async drainScope(scopeId, feature) {
    const key = `${scopeId}::${feature}`;
    const wrapper = this._limiterCache.get(key);
    if (wrapper) await wrapper.drain();
  }
  // ── Backwards-compatible aliases ─────────────────────────────────────────
  //
  // Earlier releases exposed guild-flavoured names because the library was
  // first written for a chat-bot backend. The names below keep existing
  // call sites working; the scope-flavoured names above are the canonical
  // ones and will be the ones documented going forward.
  /** @deprecated Use {@link removeScopeLimiters} instead. */
  removeGuildLimiters(guildId) {
    this.removeScopeLimiters(guildId);
  }
  /** @deprecated Use {@link drainScope} instead. */
  drainGuild(guildId, feature) {
    return this.drainScope(guildId, feature);
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
  isSentinelOptions,
  limiterManager
});
//# sourceMappingURL=index.js.map