--[[
  wakeup_check.lua — Drain the wakeup ZSET and signal workers for any
                     queues whose scheduled retry time has arrived.
                     Called by the single global ticker in WorkerPool.
                     This replaces ALL per-limiter timers.

  Wakeup members are "<poolId>|<queueId>". The poolId part identifies the
  process that owns the queue (and therefore holds the job closures), so the
  notification goes to that process' own channel.

  KEYS[1] = bq:global:wakeup    (ZSET — member → wakeup timestamp)
  KEYS[2] = bq:pools            (ZSET — live pool registry)

  ARGV[1] = now                 (current time in ms)
  ARGV[2] = batchSize           (max queues to process per tick, e.g. "100")
  ARGV[3] = liveWindowMs        (how long a pool stays live without a beat)

  Returns: number of queues signalled
--]]

local wakeupKey = KEYS[1]
local poolsKey  = KEYS[2]

local now       = ARGV[1]
local batchSize = tonumber(ARGV[2]) or 100
local liveWindowMs = tonumber(ARGV[3]) or 30000

-- Fetch all wakeup entries whose time has come (up to batchSize)
local entries = redis.call('ZRANGEBYSCORE', wakeupKey, '-inf', now, 'LIMIT', 0, batchSize)

if #entries == 0 then
  return 0
end

-- Which pools are alive right now?
local livePools = redis.call(
  'ZRANGEBYSCORE', poolsKey,
  string.format('%.0f', tonumber(now) - liveWindowMs), '+inf'
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
