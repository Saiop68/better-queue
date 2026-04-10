/* eslint-disable */
// Auto-generated file. Do not edit directly.
export const LUA = {
  complete: `--[[
  complete.lua — Mark a job as complete, release its running slot, and
                 re-signal workers if more jobs exist in the queue.

  When the queue becomes fully empty (no pending, no running) all of its
  volatile keys are immediately deleted so Redis doesn't accumulate stale
  empty structures for inactive guilds.  This is the "idle cleanup" path —
  the queue meta key is left alive (so config survives cache-miss recreation)
  but with a short TTL refreshed on every touch.

  KEYS[1] = bq:{id}:running     (SET  — currently running job IDs)
  KEYS[2] = bq:{id}:inflight    (HASH — jobId → payload, crash recovery)
  KEYS[3] = bq:{id}:jobs        (ZSET — pending jobs, checked for re-signal)
  KEYS[4] = bq:workers:notify   (LIST — worker signal channel)
  KEYS[5] = bq:{id}:meta        (HASH — queue config, TTL refreshed on activity)
  KEYS[6] = bq:{id}:lastRun     (STRING — timestamp of last job dequeue)

  ARGV[1] = jobId               (string)
  ARGV[2] = queueId             (string — for re-notification)
  ARGV[3] = ttl                 (seconds — key expiry window)

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
    -- More jobs waiting — signal workers and refresh activity TTLs
    redis.call('LPUSH', notifyKey, queueId)
    redis.call('EXPIRE', metaKey,  ttl)
    redis.call('EXPIRE', lastRunKey, ttl)
    redis.call('EXPIRE', runningKey, ttl)
    redis.call('EXPIRE', inflightKey, ttl)
    redis.call('EXPIRE', jobsKey, ttl)
  elseif running == 0 then
    -- Queue is completely idle — DELETE volatile runtime keys immediately.
    -- This frees Redis memory for inactive guilds without waiting for TTL.
    -- meta key stays alive (short TTL) so getLimiter() cold-starts cheaply.
    redis.call('DEL', runningKey)
    redis.call('DEL', inflightKey)
    redis.call('DEL', jobsKey)
    redis.call('DEL', lastRunKey)
    -- Meta survives with its current TTL — cheap to recreate, avoids syncMeta
    -- on every single member-join for guilds that are somewhat active.
    redis.call('EXPIRE', metaKey, ttl)
  end
end

return removed
`,
  dequeue: `--[[
  dequeue.lua — Atomically fetch the next eligible job from a queue,
                enforcing maxConcurrent, minTime, and reservoir constraints.
                Performs lazy reservoir refresh (no timers needed).

  KEYS[1] = bq:{id}:jobs         (ZSET   — pending jobs)
  KEYS[2] = bq:{id}:running      (SET    — currently running job IDs)
  KEYS[3] = bq:{id}:inflight     (HASH   — jobId → payload, for crash recovery)
  KEYS[4] = bq:{id}:meta         (HASH   — queue configuration)
  KEYS[5] = bq:{id}:reservoir    (STRING — current token count)
  KEYS[6] = bq:{id}:lastRun      (STRING — ms timestamp of last dequeue)
  KEYS[7] = bq:{id}:lastRefresh  (STRING — ms timestamp of last reservoir refresh)

  ARGV[1] = now                  (current time in ms)

  Returns: table { status, payload, nextEligibleMs }
    status values:
      "ok"               — payload contains job JSON, nextEligibleMs = "0"
      "empty"            — no jobs scheduled for now
      "concurrent_full"  — maxConcurrent reached, nextEligibleMs = "0"
      "rate_limited"     — minTime or reservoir not ready, nextEligibleMs = when to retry
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

-- ── Load queue meta ──────────────────────────────────────────────────────────
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

-- ── Lazy reservoir refresh ───────────────────────────────────────────────────
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

-- ── Check maxConcurrent ──────────────────────────────────────────────────────
if maxConcurrent > 0 then
  local running = tonumber(redis.call('SCARD', runningKey))
  if running >= maxConcurrent then
    return { 'concurrent_full', '', '0' }
  end
end

-- ── Check minTime ────────────────────────────────────────────────────────────
local nextByMinTime = 0
if minTime > 0 then
  local lastRun = tonumber(redis.call('GET', lastRunKey) or '0')
  local elapsed = now - lastRun
  if elapsed < minTime then
    nextByMinTime = lastRun + minTime
    return { 'rate_limited', '', tostring(nextByMinTime) }
  end
end

-- ── Check reservoir ──────────────────────────────────────────────────────────
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

-- ── Fetch next due job ───────────────────────────────────────────────────────
local jobs = redis.call('ZRANGEBYSCORE', jobsKey, '-inf', tostring(now), 'LIMIT', 0, 1)
if #jobs == 0 then
  return { 'empty', '', '0' }
end

local payload = jobs[1]

-- ── Consume reservoir token (after confirming a job exists) ──────────────────
if hasReservoir then
  redis.call('DECR', reservoirKey)
end

-- ── Move job to inflight tracking ────────────────────────────────────────────
redis.call('ZREM', jobsKey, payload)

-- Extract jobId — payload is JSON, parse the "id" field via pattern match
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
  enqueue.lua — Atomically enqueue a job into a queue's ZSET and signal workers.

  KEYS[1] = bq:{id}:jobs        (ZSET — pending jobs)
  KEYS[2] = bq:{id}:meta        (HASH  — queue config)
  KEYS[3] = bq:workers:notify   (LIST  — worker signal channel)

  ARGV[1] = jobPayload          (JSON string)
  ARGV[2] = score               (ms timestamp, 0 = use Redis server time)
  ARGV[3] = ttl                 (seconds for key expiry)
  ARGV[4] = queueId             (string — pushed to notify list)

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
  recover.lua — On startup, re-enqueue any jobs from the inflight HASH that
                are older than a visibility timeout (likely orphaned by a crash).
                Designed to be called once at startup per known queue.

  KEYS[1] = bq:{id}:inflight    (HASH — jobId → payload)
  KEYS[2] = bq:{id}:running     (SET  — running job IDs)
  KEYS[3] = bq:{id}:jobs        (ZSET — pending jobs)
  KEYS[4] = bq:workers:notify   (LIST — worker signal channel)

  ARGV[1] = now                 (current time in ms)
  ARGV[2] = visibilityTimeout   (ms — jobs older than this are considered orphaned)
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

-- Get all inflight jobs (jobId → payload pairs)
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
  stats.lua — Atomically read queue statistics from Redis.

  KEYS[1] = bq:{id}:jobs        (ZSET   — pending jobs)
  KEYS[2] = bq:{id}:running     (SET    — running job IDs)
  KEYS[3] = bq:{id}:reservoir   (STRING — current token count)

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
  wakeup_check.lua — Drain the wakeup ZSET and signal workers for any
                     queues whose scheduled retry time has arrived.
                     Called by the single global ticker in WorkerPool.
                     This replaces ALL per-limiter timers.

  KEYS[1] = bq:global:wakeup    (ZSET — queueId → wakeup timestamp)
  KEYS[2] = bq:workers:notify   (LIST — worker signal channel)

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
`,
};
