--[[
  enqueue.lua — Atomically enqueue a job into a queue and signal workers.

  A job lands in one of two ZSETs:

    bq:{id}:jobs     READY   — may run as soon as the rate limits allow.
                                score = priority * 1e12 + seq
                                (lower score runs first → priority wins,
                                 arrival order breaks ties)
    bq:{id}:delayed  WAITING — not runnable yet (scheduleAt / retry backoff).
                                score = due time in ms

  The READY score is composed server-side so priority is applied on top of
  the Redis server clock (immune to client clock skew) and so a priority-1
  job always outranks a priority-9 job regardless of arrival order.

  NOTIFICATION MODEL
  ------------------
  A job's executable function is a JS closure that lives in the process that
  scheduled it — only that process can run it. Every WorkerPool therefore
  listens on its OWN channel (`bq:workers:notify:<poolId>`), and the job's
  owner id travels inside the payload. The notification is delivered
  straight to the owner's channel, so it can never be consumed by a
  different process.

  Live pools are additionally notified (fan-out) so that a queue shared by
  several processes can be drained by whichever process owns the next job.

  KEYS[1] = bq:{id}:jobs        (ZSET  — ready jobs)
  KEYS[2] = bq:{id}:delayed     (ZSET  — not-yet-due jobs)
  KEYS[3] = bq:{id}:meta        (HASH  — queue config, holds the `seq` counter)
  KEYS[4] = bq:global:wakeup    (ZSET  — deferred worker wakeups)
  KEYS[5] = bq:pools            (ZSET  — live pool registry)

  ARGV[1] = jobPayload          (JSON string, contains the `owner` pool id)
  ARGV[2] = scheduleAt          (absolute Unix ms, 0 = run now)
  ARGV[3] = delayMs             (relative delay in ms added to server time, 0 = none)
  ARGV[4] = priority            (integer; lower runs first)
  ARGV[5] = ttl                 (seconds for key expiry)
  ARGV[6] = queueId             (string — pushed to notify list)
  ARGV[7] = liveWindowMs        (how long a pool stays live without a beat)

  Returns: the due time (ms) used for the job.
--]]

local jobsKey    = KEYS[1]
local delayedKey = KEYS[2]
local metaKey    = KEYS[3]
local wakeupKey  = KEYS[4]
local poolsKey   = KEYS[5]

local payload    = ARGV[1]
local scheduleAt = tonumber(ARGV[2]) or 0
local delayMs    = tonumber(ARGV[3]) or 0
local priority   = tonumber(ARGV[4]) or 5
local ttl        = tonumber(ARGV[5])
local queueId    = ARGV[6]
local liveWindowMs = tonumber(ARGV[7]) or 30000

-- Priority occupies the high-order digits of the score so that a priority-1
-- job always outranks a priority-9 job. 1e12 leaves room for 1e12 arrivals
-- per priority band and keeps every composed score an exact integer
-- (Redis scores are IEEE doubles → integers are exact up to 2^53).
local PRIORITY_SCALE = 1000000000000

-- Use Redis server time as the authoritative clock. This prevents clock skew
-- between the Node.js client and the Redis server from causing jobs to be
-- missed or fired early.
local _time = redis.call('TIME')
local now  = tonumber(_time[1]) * 1000 + math.floor(tonumber(_time[2]) / 1000)

-- Resolve the due time
local due = now
if scheduleAt > 0 then
  due = scheduleAt
elseif delayMs > 0 then
  due = now + delayMs
end

-- Clamp priority so the composed score can never lose precision
if priority < 0 then priority = 0 end
if priority > 99 then priority = 99 end

local function fmtScore(n)
  return string.format('%.0f', n)
end

-- The pool that owns this job (and therefore holds its closure).
local ownerPoolId = string.match(payload, '"owner"%s*:%s*"([^"]*)"')

if due > now then
  -- Not runnable yet — park it in the delayed set, ordered by due time.
  redis.call('ZADD', delayedKey, fmtScore(due), payload)
  redis.call('EXPIRE', delayedKey, ttl)

  -- Schedule a wakeup so the job fires even if every worker is busy right now.
  -- "Keep the earliest wakeup": never push an existing wakeup further out.
  local member = ownerPoolId and (ownerPoolId .. '|' .. queueId) or queueId
  local current = redis.call('ZSCORE', wakeupKey, member)
  if (not current) or (tonumber(current) > due) then
    redis.call('ZADD', wakeupKey, fmtScore(due), member)
  end
else
  -- Runnable now — put it in the ready set ordered by priority, then arrival.
  local seq   = redis.call('HINCRBY', metaKey, 'seq', 1)
  local score = priority * PRIORITY_SCALE + seq
  redis.call('ZADD', jobsKey, fmtScore(score), payload)
  redis.call('EXPIRE', jobsKey, ttl)
end

-- Refresh meta TTL (touch active queue). The `seq` counter lives here, so the
-- TTL must outlive the job set — both are refreshed together.
redis.call('EXPIRE', metaKey, ttl)

-- ── Notify the workers ──────────────────────────────────────────────────────
-- The owner MUST be notified — that is the only process able to run the job.
if ownerPoolId then
  redis.call('LPUSH', 'bq:workers:notify:' .. ownerPoolId, queueId)
end

-- Fan out to the other live pools so that a queue shared by several
-- processes can be drained by whichever process owns the next job.
local livePools = redis.call(
  'ZRANGEBYSCORE', poolsKey,
  string.format('%.0f', now - liveWindowMs), '+inf'
)
for i = 1, #livePools do
  local pid = livePools[i]
  if (not ownerPoolId) or (pid ~= ownerPoolId) then
    redis.call('LPUSH', 'bq:workers:notify:' .. pid, queueId)
  end
end

return fmtScore(due)
