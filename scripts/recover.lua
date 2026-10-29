--[[
  recover.lua — On startup, re-enqueue any jobs from the inflight HASH that
                are older than a visibility timeout (likely orphaned by a crash).
                Designed to be called once at startup per known queue.

  KEYS[1] = bq:{id}:inflight    (HASH — jobId → payload)
  KEYS[2] = bq:{id}:running     (SET  — running job IDs)
  KEYS[3] = bq:{id}:jobs        (ZSET — ready jobs)
  KEYS[4] = bq:{id}:meta        (HASH — queue config, holds the `seq` counter)
  KEYS[5] = bq:global:wakeup    (ZSET — deferred worker wakeups)

  ARGV[1] = now                 (current time in ms)
  ARGV[2] = visibilityTimeout   (ms — jobs older than this are considered orphaned)
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
