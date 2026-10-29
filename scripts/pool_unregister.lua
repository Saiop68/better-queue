--[[
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
