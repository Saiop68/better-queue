--[[
  dequeue.lua — Atomically fetch the next eligible job from a queue,
                enforcing maxConcurrent, minTime, and reservoir constraints.
                Performs lazy reservoir refresh/increase (no timers needed).

  OWNER-AWARE DEQUEUE
  -------------------
  A job's executable function is a JS closure and therefore lives in the
  process that scheduled it. A worker may therefore only run jobs that it
  owns. This script is called with the calling pool's id and scans the ready
  set for the first job owned by that pool (jobs without an `owner` field are
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

-- Find the first entry in `candidates` owned by the calling pool.
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
-- The running SET holds at most `maxConcurrent` members and the script is
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
