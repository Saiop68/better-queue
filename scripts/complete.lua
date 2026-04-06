--[[
  complete.lua — Mark a job as complete, release its running slot, and
                 re-signal workers if more jobs exist in the queue.

  KEYS[1] = bq:{id}:running     (SET  — currently running job IDs)
  KEYS[2] = bq:{id}:inflight    (HASH — jobId → payload, crash recovery)
  KEYS[3] = bq:{id}:jobs        (ZSET — pending jobs, checked for re-signal)
  KEYS[4] = bq:workers:notify   (LIST — worker signal channel)

  ARGV[1] = jobId               (string)
  ARGV[2] = queueId             (string — for re-notification)

  Returns: 1 if job was found and removed, 0 if job was not in running set
--]]

local runningKey  = KEYS[1]
local inflightKey = KEYS[2]
local jobsKey     = KEYS[3]
local notifyKey   = KEYS[4]

local jobId   = ARGV[1]
local queueId = ARGV[2]

local removed = redis.call('SREM', runningKey, jobId)
redis.call('HDEL', inflightKey, jobId)

-- Re-signal workers if there are more pending jobs in this queue
if removed == 1 then
  local pending = redis.call('ZCARD', jobsKey)
  if pending > 0 then
    redis.call('LPUSH', notifyKey, queueId)
  end
end

return removed
