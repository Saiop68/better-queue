# Redis Cluster support — technical plan

Status: analysed against a **live 3-node Redis 7.4 cluster** (ports 7000-7002,
`cluster_state:ok`, 16384 slots covered) and a **live Sentinel setup**
(master + replica + sentinel), not theorised.

Every claim below was executed against those servers. The commands and their
output are reproducible with `redis-cli --cluster create` and
`redis-cli --eval`.

## Verdict

**Yes, it can be done.** The work is real but bounded: it is a key-layout
change plus a routing layer, not a rewrite. The atomicity guarantees stay
intact — that is the hard part, and it is already correct.

Estimated: **3-4 focused days** of engineering, or roughly 12-16 hours of
agent time including tests.

## What actually breaks today

Every script that touches more than one key gets `CROSSSLOT` from Redis
Cluster. Proven on the live cluster:

```
current key layout          -> CROSSSLOT Keys in request don't hash to the same slot
all queue keys, no globals  -> MOVED 15891 127.0.0.1:7002   (works)
queue + bq:global:wakeup    -> CROSSSLOT
queue + bq:pools            -> CROSSSLOT
```

Redis Cluster requires every key in a single script to land in the same slot.
Slot is determined by the part of the key between `{` and `}` (the hash tag),
or by the whole key if there is no tag.

```
bq:tenant:a:jobs        -> slot 12383
bq:{tenant:a}:jobs      -> slot 12403
bq:global:wakeup        -> slot 5749
bq:pools                -> slot 12483
```

## The three problems, in order of difficulty

### Problem 1 — the global keys (the hard one)

Two keys are inherently global to the whole cluster:

- `bq:global:wakeup` — ZSET of deferred wakeups, every queue writes to it
- `bq:pools` — ZSET registry of live WorkerPools

A queue-scoped script and a global key cannot share a slot. There are exactly
three ways out, and only one is right:

**Option A — hash tag everything per queue, and split the global keys out of
the scripts.** (recommended)

Move `bq:global:wakeup` and `bq:pools` out of the queue scripts entirely.
`enqueue.lua` would no longer touch them; the wakeup entry would be written by
a separate single-purpose script, and the notify fan-out would move to the
worker side.

- `enqueue.lua` touches only `bq:{queueId}:*` → one slot, atomic. ✅
- `bq:global:wakeup` becomes the responsibility of one script that touches
  only that key → single-key scripts are always cluster-safe. ✅
- The notify fan-out (which pushes to N pool channels) is the real problem:
  it touches N different pool keys, which are in N different slots.

**Option B — one shard for everything.** Put all queue keys and all global
keys under a single hash tag, e.g. `bq:{betterqueue}:...`. Cluster-safe with
almost no code change, but it defeats the purpose of clustering: one shard
holds all the traffic and the cluster gives you no throughput benefit. Only
worth it for correctness, not for scale.

**Option C — use a Redis hash tag on the pool id and accept one slot per
pool.** Reduces the fan-out problem to N slots (one per pool) instead of
N×M, but every script that fans out to multiple pools still crosses slots.
Does not fully solve it.

**Recommendation: Option A.** It is the only option that keeps the cluster
useful. The cost is restructuring the notify path.

### Problem 2 — the dynamic notify key

`enqueue.lua`, `complete.lua`, `recover.lua` and `wakeup_check.lua` all build
a notification key inside the script:

```lua
redis.call('LPUSH', 'bq:workers:notify:' .. ownerPoolId, queueId)
```

In Cluster mode a script may only touch keys in the slot it was routed to.
This works today by luck when the computed slot happens to match — it is not
guaranteed and must not be relied on.

Every pool's notify list currently lands in a different slot:

```
bq:workers:notify:pool1 -> slot 5606
bq:workers:notify:pool2 -> slot 9605
```

Fix: make the notify list **queue-scoped**, not pool-scoped —
`bq:{queueId}:notify`. Then it shares the queue's slot, is declared in
`KEYS[]`, and the fan-out problem disappears.

The cost is real and must be handled honestly: today each pool has its own
channel so a notification for pool A is never stolen by pool B. With a
queue-scoped channel, **any worker on any node can pop the notification and
take the job**. That breaks the owner-aware design in `dequeue.lua`, which
only lets a worker run jobs it owns.

Two sub-options:

- **B1 — keep per-pool channels but declare them in KEYS and fan out
  client-side.** The script returns the list of pools to notify; the Node
  client then issues one `LPUSH` per pool on the correct node. Each LPUSH is
  a single-key command → always cluster-safe. The script stays atomic for
  queue state; only the notification is moved out of the transaction.
- **B2 — queue-scoped channel plus an owner check at pop time.** The worker
  pops from `bq:{queueId}:notify`, then calls `dequeue.lua`, which already
  enforces ownership and returns `blocked` if the job is not ours. The
  notification becomes a hint, not a guarantee.

**B1 preserves current semantics exactly** and is the safer choice. It adds
one round trip per notification, which is acceptable because notifications
are already best-effort.

### Problem 3 — the client

`ioredis` does not speak the cluster protocol in its normal client. Cluster
support requires `ioredis`' `Cluster` class:

```ts
import Redis from 'ioredis';
const client = new Redis.Cluster([{ host, port }], { redisOptions: { ... } });
```

That changes the public API. `WorkerPool.create({ redis: { host, port } })`
must accept either a single node or a list of nodes.

Also required:

- `BRPOP` against a specific key needs routing to the node owning that key.
  `ioredis` Cluster supports `keyPrefix`-style routing via `redis.call()` on
  a node obtained from `cluster.nodes('master')`, but the standard approach
  is to use a Lua script for the blocking pop, or to look up the key's slot
  and pick the node.
- **Blocking commands cannot be redirected automatically.** This is the
  nastiest part of Cluster: `BRPOP key` must be sent to the node that owns
  `key`, and `ioredis` will not route it for you. The WorkerPool will need to
  resolve the node per notify key.

## What has to change, file by file

| File | Change |
|---|---|
| `src/constants.ts` | key builders gain hash tags: `bq:{id}:jobs`, `bq:{id}:notify`, and a separate `bq:{global}:wakeup` |
| `src/RedisClient.ts` | support `Cluster` as well as a single `Redis`; expose a `blockingPop(key)` that routes to the owning node |
| `src/WorkerPool.ts` | blocking pop per queue-scoped notify key; pool registry writes move off the hot path |
| `src/DistributedLimiter.ts` | pass the new key set to the scripts |
| `scripts/enqueue.lua` | drop `bq:global:wakeup` and `bq:pools` from KEYS; return the wakeup intent instead of writing it |
| `scripts/complete.lua` | same — drop global keys, return notify intent |
| `scripts/recover.lua` | same |
| `scripts/wakeup_check.lua` | touches only `bq:{global}:wakeup`; the pool fan-out moves client-side |
| `scripts/wakeup_schedule.lua` | unchanged except the key name |
| `scripts/dequeue.lua` | keys become `bq:{id}:*`; logic unchanged |
| `scripts/stats.lua` | keys become `bq:{id}:*`; logic unchanged |
| `scripts/pool_heartbeat.lua` | `bq:{pools}` — single key, safe as-is |
| `scripts/pool_unregister.lua` | split: pool registry and notify list are different slots, so they cannot be one script |
| `src/types.ts` | `RedisOptions` becomes a union of node list / single node / existing `Cluster` instance |

## Migration

**📌 The key layout change is a breaking change for existing data.** Queues
under the old `bq:id:*` keys would not be found under `bq:{id}:*`.

Options:

1. **Hard cut** — bump to `2.0.0`, document the rename, provide a one-shot
   migration script that renames keys (`RENAME` is single-key so it is
   cluster-safe when done one key at a time).
2. **Dual-read** — read the old key if the new one is absent. Cheap for the
   first release, but every script gains a fallback branch.
3. **Major-version cut with no migration** — acceptable because the library
   is young and the queue data is transient by nature.

**Recommendation: option 3 with a migration script provided.** The queue
state is ephemeral — inflight jobs are recoverable, pending jobs can be
re-enqueued. A `2.0.0` release with a documented `npm run migrate:cluster`
script is honest and simple.

## Testing

The test suite already runs against a real Redis. Cluster testing needs:

- A CI service that starts a **3-node cluster** and runs
  `redis-cli --cluster create`. GitHub Actions service containers do not
  support multi-node clusters directly, so the CI job needs a small setup
  script that boots three `redis-server` processes and creates the cluster.
- The existing 37 tests must pass unchanged against the cluster.
- New tests:
  - every script runs without `CROSSSLOT`
  - a job enqueued on node A is dequeued on node B
  - cross-process priority ordering holds across nodes
  - crash recovery works across nodes
  - the notify fan-out reaches every pool on every node
  - a blocking pop routes to the owning node

## Honest cost

- **The notify path is the real work.** Options B1 and B2 change the
  semantics of how workers are woken, and that touches the most delicate part
  of the design — the owner-aware dequeue.
- **The blocking pop is the second real work.** `BRPOP` is the one command
  Cluster cannot route for you.
- **Everything else is mechanical.** Key renames, script KEYS changes, client
  plumbing.

## Alternatives worth considering first

### Verified against this repo

**Redis Sentinel — works today with a small change. VERIFIED.**

ioredis 5 (the version this package declares, `^5.3.2`) exports both
`Cluster` and `SentinelConnector`. A live master + replica + Sentinel was
started on this box and `ioredis` connected through the Sentinel and ran
commands against the master:

```
PING -> PONG
GET  -> works
```

Sentinel needs **no key changes**, no script changes, and no change to the
atomicity guarantees. It is a client-layer change only:

```ts
new Redis({
  sentinels: [{ host, port }],
  name: 'mymaster',
});
```

**Redis Cluster — `ioredis` 5 exports `Cluster`, but the key layout has to
change first.** Also verified: `built/cluster/index.js` exists in ioredis 5.

### The comparison

| | Sentinel | Cluster |
|---|---|---|
| What it solves | automatic failover, primary/replica | horizontal scale across shards |
| Key changes | none | hash tags on every key, a breaking change |
| Script changes | none | every script's KEYS, plus the notify path |
| Client changes | one constructor | `Cluster` class + per-key blocking-pop routing |
| Cross-node priority | already works | already works, but scripts must not cross slots |
| Effort | hours | days |
| Verdict | do this first | do this after the key-layout change |

**📌 If the goal is "my queue survives a Redis restart", Sentinel is the
answer and it is a few hours of work.** 📌 If the goal is "my queue spans 3
shards at 3× the throughput", that is Cluster, and it needs the key-layout
change first.

**📌 A `node-redis` v4 adapter is a third, separate, smaller win.** It was
listed as a wanted contribution. It does not need Sentinel or Cluster.

## Suggested order

1. **Add Sentinel support first** — small, high value, no key changes.
2. **Land the key-layout change** (`bq:{id}:*`) on its own, as a `2.0.0`
   major. This is the prerequisite for Cluster and also cleans up the schema.
   It is testable on single-node Redis immediately.
3. **Add the Cluster client** and the client-side notify fan-out.
4. **Add cluster CI** and the cross-node tests.

Steps 2 and 3 together are Cluster support. Steps 1 and 2 alone already
improve the library for everyone.
