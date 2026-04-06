--[[
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
