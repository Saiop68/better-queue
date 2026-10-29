/* eslint-disable */
// Auto-generated file. Do not edit directly.
export const LUA = {
  complete: `--[[
  complete.lua — Mark a job as complete, release its running slot, and
                 re-signal workers if more jobs exist in the queue.

  When the queue becomes fully empty (no pending, no running) all of its
  volatile keys are immediately deleted so Redis doesn't accumulate stale
  empty structures for inactive guilds.  This is the "idle cleanup" path —
  the queue meta key is left alive (so config survives cache-miss recreation)
  but with a short TTL refreshed on every touch.

  KEYS[1] = bq:{id}:running     (SET  — currently running job IDs)
  KEYS[2] = bq:{id}:inflight    (HASH — jobId → payload, crash recovery)
  KEYS[3] = bq:{id}:jobs        (ZSET — ready jobs)
  KEYS[4] = bq:{id}:meta        (HASH — queue config, TTL refreshed on activity)
  KEYS[5] = bq:{id}:lastRun     (STRING — timestamp of last job dequeue)
  KEYS[6] = bq:{id}:delayed     (ZSET — not-yet-due jobs)
  KEYS[7] = bq:global:wakeup    (ZSET — deferred worker wakeups)
  KEYS[8] = bq:pools            (ZSET — live pool registry)

  ARGV[1] = jobId               (string)
  ARGV[2] = queueId             (string — for re-notification)
  ARGV[3] = ttl                 (seconds — key expiry window)
  ARGV[4] = ownerPoolId         (pool id of the process that owns the queue)
  ARGV[5] = liveWindowMs        (how long a pool stays live without a beat)

  Returns: 1 if job was found and removed, 0 if job was not in running set
--]]

local runningKey  = KEYS[1]
local inflightKey = KEYS[2]
local jobsKey     = KEYS[3]
local metaKey     = KEYS[4]
local lastRunKey  = KEYS[5]
local delayedKey  = KEYS[6]
local wakeupKey   = KEYS[7]
local poolsKey    = KEYS[8]

local jobId   = ARGV[1]
local queueId = ARGV[2]
local ttl     = tonumber(ARGV[3])
local ownerPoolId = ARGV[4]
local liveWindowMs = tonumber(ARGV[5]) or 30000

local removed = redis.call('SREM', runningKey, jobId)
redis.call('HDEL', inflightKey, jobId)

if removed == 1 then
  local pending = redis.call('ZCARD', jobsKey) + redis.call('ZCARD', delayedKey)
  local running = redis.call('SCARD', runningKey)

  if pending > 0 then
    -- More jobs waiting — signal the owner and refresh activity TTLs.
    if ownerPoolId then
      redis.call('LPUSH', 'bq:workers:notify:' .. ownerPoolId, queueId)
    end

    -- A freed concurrency slot can unblock a job owned by a DIFFERENT
    -- process. Notify every other live pool as well, so a job waiting on a
    -- slot held elsewhere starts as soon as the slot is released instead of
    -- waiting for the next poll tick. (Mirrors the fan-out in enqueue.lua.)
    --
    -- Only queues that are actually concurrency-limited do this — an
    -- unlimited queue never has a waiter blocked on a slot, so it would
    -- just be noise.
    local maxConcurrent = tonumber(redis.call('HGET', metaKey, 'maxConcurrent') or '0')
    if maxConcurrent > 0 then
      local _time = redis.call('TIME')
      local now  = tonumber(_time[1]) * 1000 + math.floor(tonumber(_time[2]) / 1000)
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
    end

    redis.call('EXPIRE', metaKey,  ttl)
    redis.call('EXPIRE', lastRunKey, ttl)
    redis.call('EXPIRE', runningKey, ttl)
    redis.call('EXPIRE', inflightKey, ttl)
    redis.call('EXPIRE', jobsKey, ttl)
    redis.call('EXPIRE', delayedKey, ttl)
  elseif running == 0 then
    -- Queue is completely idle — DELETE volatile runtime keys immediately.
    -- This frees Redis memory for inactive guilds without waiting for TTL.
    -- meta key stays alive (short TTL) so getLimiter() cold-starts cheaply.
    redis.call('DEL', runningKey)
    redis.call('DEL', inflightKey)
    redis.call('DEL', jobsKey)
    redis.call('DEL', delayedKey)
    redis.call('DEL', lastRunKey)
    if ownerPoolId then
      redis.call('ZREM', wakeupKey, ownerPoolId .. '|' .. queueId)
    end
    redis.call('ZREM', wakeupKey, queueId)
    -- Meta survives with its current TTL — cheap to recreate, avoids syncMeta
    -- on every single member-join for guilds that are somewhat active.
    redis.call('EXPIRE', metaKey, ttl)
  end
end

return removed
`,
  dequeue: `--[[
  dequeue.lua — Atomically fetch the next eligible job from a queue,
                enforcing maxConcurrent, minTime, and reservoir constraints.
                Performs lazy reservoir refresh/increase (no timers needed).

  OWNER-AWARE DEQUEUE
  -------------------
  A job's executable function is a JS closure and therefore lives in the
  process that scheduled it. A worker may therefore only run jobs that it
  owns. This script is called with the calling pool's id and scans the ready
  set for the first job owned by that pool (jobs without an \`owner\` field are
  claimable by anyone, for backwards compatibility).

  Rate-limit accounting (maxConcurrent / minTime / reservoir) is GLOBAL —
  it is shared by every process working on the same queue id, which is what
  makes the limiter meaningful across a cluster.

  KEYS[1] = bq:{id}:jobs         (ZSET   — ready jobs, score = priority*1e12 + seq)
  KEYS[2] = bq:{id}:delayed      (ZSET   — not-yet-due jobs, score = due time ms)
  KEYS[3] = bq:{id}:running      (SET    — currently running job IDs)
  KEYS[4] = bq:{id}:inflight     (HASH   — jobId → payload, for crash recovery)
  KEYS[5] = bq:{id}:meta         (HASH   — queue configuration)
  KEYS[6] = bq:{id}:reservoir    (STRING — current token count)
  KEYS[7] = bq:{id}:lastRun      (STRING — ms timestamp of last dequeue)
  KEYS[8] = bq:{id}:lastRefresh  (STRING — ms timestamp of last reservoir refresh)
  KEYS[9] = bq:{id}:lastIncrease (STRING — ms timestamp of last reservoir increase)

  ARGV[1] = now                  (client clock, used only as a fallback)
  ARGV[2] = ttl                  (seconds, fallback when meta has none)
  ARGV[3] = ownerId              (pool id of the calling worker)
  ARGV[4] = scanWindow          (max entries to inspect per ZSET)

  Returns: table { status, payload, nextEligibleMs }
    status values:
      "ok"               — payload contains job JSON, nextEligibleMs = "0"
      "empty"            — nothing of ours is pending
      "blocked"          — our jobs exist but are further down the queue;
                            nextEligibleMs = when to look again
      "concurrent_full"  — every concurrency slot is taken (possibly by a
                            job owned by ANOTHER process) and we have a job
                            ready to run; nextEligibleMs = when to retry
      "rate_limited"     — minTime or reservoir not ready, nextEligibleMs = when to retry
--]]

local PRIORITY_SCALE = 1000000000000
local DEFAULT_SCAN_WINDOW = 256
-- How long to wait before looking again when every concurrency slot is
-- taken. We cannot know when the holder of the slot finishes, so this is a
-- short poll — the same cadence the "blocked" status already uses.
local CONCURRENCY_RETRY_MS = 50
-- How many entries of the running SET to inspect when working out whether
-- a concurrency slot is held by a job of this pool. Bounded so the check
-- stays O(1)-ish on large queues.
local MAX_OWNERSHIP_INSPECT = 32

local jobsKey         = KEYS[1]
local delayedKey      = KEYS[2]
local runningKey      = KEYS[3]
local inflightKey     = KEYS[4]
local metaKey         = KEYS[5]
local reservoirKey    = KEYS[6]
local lastRunKey      = KEYS[7]
local lastRefreshKey  = KEYS[8]
local lastIncreaseKey = KEYS[9]

local ownerId     = ARGV[3]
local scanWindow  = tonumber(ARGV[4]) or DEFAULT_SCAN_WINDOW
if scanWindow < 1 then scanWindow = 1 end

local function fmtScore(n)
  return string.format('%.0f', n)
end

-- Extract the owner of a job payload. Jobs without an owner field are
-- claimable by any worker (legacy payloads).
local function ownerOf(payload)
  return string.match(payload, '"owner"%s*:%s*"([^"]*)"')
end

-- Find the first entry in \`candidates\` owned by the calling pool.
local function firstOwned(candidates)
  for i = 1, #candidates do
    local p = candidates[i]
    local o = ownerOf(p)
    if (not o) or (o == ownerId) then
      return p
    end
  end
  return nil
end

-- Are ALL currently running jobs owned by the calling pool?
--
-- Used to tell two "the queue is full" situations apart:
--   * all ours → every one of those jobs will notify this pool when it
--                completes (complete.lua notifies the owner), so no polling
--                is needed. This keeps the single-process path exactly as
--                it was: no polling at all.
--   * not all ours → at least one slot is held by ANOTHER process. We
--                cannot know when it will be released, so we must poll.
--
-- The running SET holds at most \`maxConcurrent\` members and the script is
-- atomic, so the scan is exact. SSCAN is used instead of SMEMBERS so the
-- amount of data transferred stays bounded even for very large slots.
--
-- Any uncertainty (more entries than we are willing to inspect, or a
-- missing inflight record) is resolved as "not all ours" — polling is
-- always safe, it is only ever slightly more Redis traffic.
local function runningAllOurs(runningKey, inflightKey)
  local cursor = '0'
  local checked = 0
  repeat
    local res = redis.call('SSCAN', runningKey, cursor, 'COUNT', 16)
    cursor = res[1]
    local members = res[2]
    for i = 1, #members do
      checked = checked + 1
      if checked > MAX_OWNERSHIP_INSPECT then
        return false
      end
      local runningPayload = redis.call('HGET', inflightKey, members[i])
      if not runningPayload then
        return false
      end
      if ownerOf(runningPayload) ~= ownerId then
        return false
      end
    end
  until cursor == '0'
  return true
end

-- ── Load queue meta ──────────────────────────────────────────────────────────
local metaArr = redis.call('HGETALL', metaKey)
local meta = {}
for i = 1, #metaArr, 2 do
  meta[metaArr[i]] = metaArr[i + 1]
end

local maxConcurrent       = tonumber(meta['maxConcurrent'] or '0')
local minTime             = tonumber(meta['minTime'] or '0')
local ttl                 = tonumber(meta['ttl'] or ARGV[2] or '86400')
local refreshInterval     = tonumber(meta['reservoirRefreshInterval'] or '0')
local refreshAmount       = tonumber(meta['reservoirRefreshAmount'] or '0')
local increaseInterval    = tonumber(meta['reservoirIncreaseInterval'] or '0')
local increaseAmount      = tonumber(meta['reservoirIncreaseAmount'] or '0')
local increaseMaximum     = tonumber(meta['reservoirIncreaseMaximum'] or '0')
local hasReservoir        = meta['reservoir'] ~= nil and meta['reservoir'] ~= '-1'

-- Use Redis server time as the authoritative clock. This prevents clock skew
-- between the Node.js client and the Redis server from causing jobs to be
-- missed (enqueued with server time, dequeued with client time).
local _time = redis.call('TIME')
local now  = tonumber(_time[1]) * 1000 + math.floor(tonumber(_time[2]) / 1000)

-- ── Promote due delayed jobs into the ready set ─────────────────────────────
-- A delayed job becomes runnable exactly when its due time passes. Promoting
-- it here keeps the whole transition atomic with the dequeue that runs it.
local dueJobs = redis.call('ZRANGEBYSCORE', delayedKey, '-inf', fmtScore(now))
if #dueJobs > 0 then
  for i = 1, #dueJobs do
    redis.call('ZREM', delayedKey, dueJobs[i])
    local prio = tonumber(string.match(dueJobs[i], '"priority"%s*:%s*(%d+)')) or 5
    if prio < 0 then prio = 0 end
    if prio > 99 then prio = 99 end
    local seq   = redis.call('HINCRBY', metaKey, 'seq', 1)
    local score = prio * PRIORITY_SCALE + seq
    redis.call('ZADD', jobsKey, fmtScore(score), dueJobs[i])
  end
  redis.call('EXPIRE', jobsKey, ttl)
  redis.call('EXPIRE', delayedKey, ttl)
end

-- ── Lazy reservoir refresh + increase ────────────────────────────────────────
-- Instead of a timer, we check on every dequeue attempt whether an interval
-- has elapsed and update the token bucket atomically.
--
--   refresh  → reservoir := reservoirRefreshAmount   (a RESET)
--   increase → reservoir := reservoir + reservoirIncreaseAmount (an ADD),
--              capped at reservoirIncreaseMaximum when configured.
local tokens = 0
if hasReservoir then
  local raw = redis.call('GET', reservoirKey)
  if raw then
    tokens = tonumber(raw)
  else
    -- The reservoir key expired (TTL) or was never created — re-seed it with
    -- the configured bucket size so the queue does not stay blocked forever.
    tokens = tonumber(meta['reservoir'] or '0')
    redis.call('SET', reservoirKey, tostring(tokens))
  end

  -- Refresh (RESET) and increase (ADD) are mutually exclusive: the refresh
  -- wins if both are configured, matching Bottleneck's behaviour.
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
end

-- ── Compute the earliest time a job could run ───────────────────────────────
local readyAt = now

if minTime > 0 then
  local lastRun = tonumber(redis.call('GET', lastRunKey) or '0')
  if (now - lastRun) < minTime then
    readyAt = lastRun + minTime
  end
end

if hasReservoir and tokens <= 0 then
  -- Calculate when the next token(s) arrive
  local nextToken = 0
  if refreshInterval > 0 and refreshAmount > 0 then
    local lastRefresh = tonumber(redis.call('GET', lastRefreshKey) or '0')
    nextToken = lastRefresh + refreshInterval
  elseif increaseInterval > 0 and increaseAmount > 0 then
    local lastIncrease = tonumber(redis.call('GET', lastIncreaseKey) or '0')
    nextToken = lastIncrease + increaseInterval
  end
  if nextToken == 0 then nextToken = now + 1000 end
  if nextToken > readyAt then readyAt = nextToken end
end

-- ── Fetch the next ready job owned by this pool ─────────────────────────────
local candidates = redis.call('ZRANGE', jobsKey, 0, scanWindow - 1)
local payload = firstOwned(candidates)

if payload then
  -- ── Concurrency check ──────────────────────────────────────────────────────
  -- We only reach this point with a job of OURS that we are about to run, so
  -- the check applies to the job this pool is actually taking — not to some
  -- unrelated job that happens to occupy a slot.
  if maxConcurrent > 0 then
    local running = tonumber(redis.call('SCARD', runningKey))
    if running >= maxConcurrent then
      -- Every slot is taken. Find out whether ALL of them are held by jobs
      -- of THIS pool:
      --
      --   * all ours  → when those jobs finish, complete.lua notifies this
      --                 pool directly, so no wakeup is needed (nextEligibleMs
      --                 = 0). This keeps the single-process path exactly as
      --                 it was: no polling at all.
      --   * not ours  → at least one slot belongs to ANOTHER process. We
      --                 cannot know when it will be released, so ask to be
      --                 woken up again shortly. Returning "no idea when to
      --                 retry" here is what used to strand the job in Redis
      --                 until an unrelated notification poked the queue.
      if runningAllOurs(runningKey, inflightKey) then
        return { 'concurrent_full', '', '0' }
      end
      return { 'concurrent_full', '', fmtScore(now + CONCURRENCY_RETRY_MS) }
    end
  end

  if readyAt > now then
    -- A ready job exists but minTime / reservoir says it cannot start yet.
    return { 'rate_limited', '', tostring(readyAt) }
  end

  -- ── Consume reservoir token ──────────────────────────────────────────────
  if hasReservoir then
    redis.call('DECR', reservoirKey)
  end

  -- ── Move job to inflight tracking ────────────────────────────────────────
  redis.call('ZREM', jobsKey, payload)

  -- Extract jobId — payload is JSON, parse the "id" field via pattern match
  -- Pattern: ..."id":"<uuid>"... where uuid is alphanumeric-dash
  local jobId = string.match(payload, '"id"%s*:%s*"([^"]+)"')

  if jobId then
    redis.call('SADD', runningKey, jobId)
    redis.call('EXPIRE', runningKey, ttl)
    redis.call('HSET', inflightKey, jobId, payload)
    redis.call('EXPIRE', inflightKey, ttl)
  end

  -- Update lastRun timestamp
  redis.call('SET', lastRunKey, tostring(now))
  redis.call('EXPIRE', lastRunKey, ttl)

  return { 'ok', payload, '0' }
end

-- ── Nothing of ours is ready — check for our own delayed jobs ──────────────
local delayedCandidates = redis.call('ZRANGE', delayedKey, 0, scanWindow - 1, 'WITHSCORES')
local myDelayed = nil
local myDelayedScore = nil
for i = 1, #delayedCandidates, 2 do
  local p = delayedCandidates[i]
  local o = ownerOf(p)
  if (not o) or (o == ownerId) then
    myDelayed = p
    myDelayedScore = tonumber(delayedCandidates[i + 1])
    break
  end
end

if myDelayed then
  local due = myDelayedScore
  if due < now then due = now end
  if due > readyAt then due = readyAt end
  return { 'empty', '', tostring(due) }
end

-- ── Are our jobs further down the queue? ───────────────────────────────────
-- The ready set was full of other processes' jobs. If it is not exhausted
-- there may be one of ours further down — report "blocked" so the caller can
-- come back shortly.
if #candidates >= scanWindow then
  return { 'blocked', '', tostring(now + 50) }
end

return { 'empty', '', '0' }
`,
  enqueue: `--[[
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
  listens on its OWN channel (\`bq:workers:notify:<poolId>\`), and the job's
  owner id travels inside the payload. The notification is delivered
  straight to the owner's channel, so it can never be consumed by a
  different process.

  Live pools are additionally notified (fan-out) so that a queue shared by
  several processes can be drained by whichever process owns the next job.

  KEYS[1] = bq:{id}:jobs        (ZSET  — ready jobs)
  KEYS[2] = bq:{id}:delayed     (ZSET  — not-yet-due jobs)
  KEYS[3] = bq:{id}:meta        (HASH  — queue config, holds the \`seq\` counter)
  KEYS[4] = bq:global:wakeup    (ZSET  — deferred worker wakeups)
  KEYS[5] = bq:pools            (ZSET  — live pool registry)

  ARGV[1] = jobPayload          (JSON string, contains the \`owner\` pool id)
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

-- Refresh meta TTL (touch active queue). The \`seq\` counter lives here, so the
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
`,
  poolHeartbeat: `--[[
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
`,
  poolUnregister: `--[[
  pool_unregister.lua — Remove a WorkerPool from the live registry and clean
                        up its notification channel.

  Called when a pool shuts down gracefully, so other processes stop
  notifying it immediately instead of waiting for the liveness window to
  expire.

  KEYS[1] = bq:pools            (ZSET — poolId → last heartbeat ms)
  KEYS[2] = bq:workers:notify:<poolId>  (LIST — this pool's notify channel)

  ARGV[1] = poolId              (string)

  Returns: 1 if the pool was registered, 0 otherwise.
--]]

local poolsKey = KEYS[1]
local notifyKey = KEYS[2]
local poolId = ARGV[1]

local removed = redis.call('ZREM', poolsKey, poolId)
redis.call('DEL', notifyKey)

return removed
`,
  recover: `--[[
  recover.lua — On startup, re-enqueue any jobs from the inflight HASH that
                are older than a visibility timeout (likely orphaned by a crash).
                Designed to be called once at startup per known queue.

  KEYS[1] = bq:{id}:inflight    (HASH — jobId → payload)
  KEYS[2] = bq:{id}:running     (SET  — running job IDs)
  KEYS[3] = bq:{id}:jobs        (ZSET — ready jobs)
  KEYS[4] = bq:{id}:meta        (HASH — queue config, holds the \`seq\` counter)
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
`,
  stats: `--[[
  stats.lua — Atomically read queue statistics from Redis.

  Applies the same lazy reservoir maintenance as dequeue.lua, so the reported
  token count is accurate even when the queue is idle (no dequeue in flight).
  This keeps \`getReservoir()\` honest without adding a per-limiter timer.

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
`,
  wakeupCheck: `--[[
  wakeup_check.lua — Drain the wakeup ZSET and signal workers for any
                     queues whose scheduled retry time has arrived.
                     Called by the single global ticker in WorkerPool.
                     This replaces ALL per-limiter timers.

  Wakeup members are "<poolId>|<queueId>". The poolId part identifies the
  process that owns the queue (and therefore holds the job closures), so the
  notification goes to that process' own channel.

  KEYS[1] = bq:global:wakeup    (ZSET — member → wakeup timestamp)
  KEYS[2] = bq:pools            (ZSET — live pool registry)

  ARGV[1] = now                 (ignored — the server clock is used instead)
  ARGV[2] = batchSize           (max queues to process per tick, e.g. "100")
  ARGV[3] = liveWindowMs        (how long a pool stays live without a beat)

  Returns: number of queues signalled
--]]

local wakeupKey = KEYS[1]
local poolsKey  = KEYS[2]

local batchSize = tonumber(ARGV[2]) or 100
local liveWindowMs = tonumber(ARGV[3]) or 30000

-- Use the Redis server clock as the authoritative "now".
--
-- Every wakeup score is written server-side (enqueue.lua, dequeue.lua), so
-- comparing against the server clock keeps the two in the same time base.
-- Using the client's clock here would let a client whose clock runs ahead
-- of the server wake up immediately after scheduling, turning the retry
-- interval into a hot loop.
local _time = redis.call('TIME')
local now   = tonumber(_time[1]) * 1000 + math.floor(tonumber(_time[2]) / 1000)
local nowStr = string.format('%.0f', now)

-- Fetch all wakeup entries whose time has come (up to batchSize)
local entries = redis.call('ZRANGEBYSCORE', wakeupKey, '-inf', nowStr, 'LIMIT', 0, batchSize)

if #entries == 0 then
  return 0
end

-- Which pools are alive right now?
local livePools = redis.call(
  'ZRANGEBYSCORE', poolsKey,
  string.format('%.0f', now - liveWindowMs), '+inf'
)

local function isLive(pid)
  for i = 1, #livePools do
    if livePools[i] == pid then return true end
  end
  return false
end

local signalled = 0

for _, member in ipairs(entries) do
  -- Remove only the entries we actually process — otherwise entries beyond
  -- the batch size would be silently dropped without ever being woken up.
  redis.call('ZREM', wakeupKey, member)

  local sep = string.find(member, '|', 1, true)
  local pid, queueId
  if sep then
    pid      = string.sub(member, 1, sep - 1)
    queueId  = string.sub(member, sep + 1)
  else
    pid      = nil
    queueId  = member
  end

  if pid then
    -- Notify the owning pool directly.
    redis.call('LPUSH', 'bq:workers:notify:' .. pid, queueId)
    signalled = signalled + 1
  else
    -- Legacy member without an owner — notify every live pool.
    for i = 1, #livePools do
      redis.call('LPUSH', 'bq:workers:notify:' .. livePools[i], queueId)
    end
    if #livePools > 0 then signalled = signalled + 1 end
  end
end

return signalled
`,
  wakeupSchedule: `--[[
  wakeup_schedule.lua — Atomically schedule a worker wakeup for a queue.

  The wakeup ZSET is a global registry of "queues that need to be looked at
  again later". A worker that could not run a job right now (rate limited,
  delayed, blocked by another process, or out of concurrency slots) parks
  the queue here so the global ticker wakes it up again at the right time.

  Entries are keyed by "<poolId>|<queueId>" so the ticker can notify the
  exact pool that asked for the wakeup — the pool that holds the job
  closures. One member per (pool, queue) pair means a queue can never
  accumulate more than a single pending wakeup.

  This script keeps the EARLIEST wakeup: an existing entry that fires
  sooner is never pushed back. Doing that atomically (inside Lua) avoids
  the race where two concurrent wakeup requests for the same queue read a
  stale score and one of them overwrites the other's earlier time — which
  could delay a job that was ready to run.

  KEYS[1] = bq:global:wakeup    (ZSET — member → wakeup timestamp in ms)

  ARGV[1] = member              ("<poolId>|<queueId>")
  ARGV[2] = atMs                (wakeup time in ms)

  Returns: 1 if the entry was created or moved earlier, 0 if an earlier
           entry was kept.
--]]

local wakeupKey = KEYS[1]
local member    = ARGV[1]
local atMs      = tonumber(ARGV[2])

if (not member) or (not atMs) then
  return 0
end

local current = redis.call('ZSCORE', wakeupKey, member)
if (not current) or (tonumber(current) > atMs) then
  redis.call('ZADD', wakeupKey, string.format('%.0f', atMs), member)
  return 1
end

return 0
`,
};
