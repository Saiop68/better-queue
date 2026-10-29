--[[
  stats.lua — Atomically read queue statistics from Redis.

  Applies the same lazy reservoir maintenance as dequeue.lua, so the reported
  token count is accurate even when the queue is idle (no dequeue in flight).
  This keeps `getReservoir()` honest without adding a per-limiter timer.

  KEYS[1] = bq:{id}:jobs        (ZSET   — ready jobs)
  KEYS[2] = bq:{id}:running     (SET    — running job IDs)
  KEYS[3] = bq:{id}:reservoir   (STRING — current token count)
  KEYS[4] = bq:{id}:delayed     (ZSET   — not-yet-due jobs)
  KEYS[5] = bq:{id}:meta        (HASH   — queue configuration)
  KEYS[6] = bq:{id}:lastRefresh (STRING — ms timestamp of last reservoir refresh)
  KEYS[7] = bq:{id}:lastIncrease (STRING — ms timestamp of last reservoir increase)

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

-- ── Load queue meta ──────────────────────────────────────────────────────────
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
    -- Never seeded (or expired) — start from the configured bucket size.
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
