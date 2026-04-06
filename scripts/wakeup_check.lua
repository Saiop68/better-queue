--[[
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
