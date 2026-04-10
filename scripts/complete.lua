--[[
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
