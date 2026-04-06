--[[
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
