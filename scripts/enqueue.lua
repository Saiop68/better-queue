--[[
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
