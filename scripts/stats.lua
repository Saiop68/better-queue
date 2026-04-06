--[[
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
