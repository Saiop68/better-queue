--[[
  pool_heartbeat.lua — Register/refresh this WorkerPool's liveness in the
                       shared pool registry.

  The registry (bq:pools) is a ZSET whose score is the pool's last heartbeat
  timestamp (Redis server time). Pools that stop heartbeating fall out of the
  "live" window automatically, so the registry self-cleans — no GC needed.

  KEYS[1] = bq:pools            (ZSET — poolId → last heartbeat ms)

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
