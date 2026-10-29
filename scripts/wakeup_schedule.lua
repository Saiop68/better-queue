--[[
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
