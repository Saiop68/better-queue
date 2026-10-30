# Contributing to BetterQueue

Thanks for considering a contribution. This document is short on purpose — if
something here is wrong or out of date, please open an issue saying so.

## Getting set up

```bash
git clone https://github.com/Better-Queue/better-queue.git
cd better-queue
npm install
npm run build:lua   # inlines scripts/*.lua into src/lua-scripts.ts
npm run build
npm test
```

Tests need a Redis instance on `127.0.0.1:6379`. Any Redis 6+ works; a
throwaway one is fine:

```bash
docker run --rm -p 6379:6379 redis:7-alpine
```

## The one rule that matters

**Lua scripts and TypeScript must be changed together.**

`scripts/*.lua` is the source of truth for anything that touches Redis state.
`src/lua-scripts.ts` is generated from it by `npm run build:lua` and is what
actually ships. If you edit a `.lua` file without regenerating, the change
silently does nothing.

If your PR touches `scripts/`, the diff should include the regenerated
`src/lua-scripts.ts`. CI checks this by rebuilding and running the suite.

## Before you open a PR

```bash
npm run build:lua
npm run build
npx tsc --noEmit
npm test
```

CI runs the same four commands on Node 18, 20 and 22. All three must pass.

## Style

- TypeScript, ESM, 4-space indent, single quotes, semicolons.
- Imports use the `.js` extension (`import { x } from './thing.js'`) because the
  package is ESM.
- Keep the Lua atomic. Every state transition that could race belongs in one
  script, not split across a read and a write.

## Good first contributions

Things we know are missing and would happily accept a PR for:

- **Redis Cluster support.** The scripts touch several keys at once and the key
  names have no hash tag, so clustered Redis gives you `CROSSSLOT`. Adding a
  hash tag to the key scheme (or a separate key layout per slot) would fix it.
- **A `node-redis` v4 adapter.** The library is `ioredis`-only right now.
  The adapter surface is small — `RedisClient.ts` is the only file that talks
  to the driver.
- **`reset()` / `flush()`.** Clear a queue's state in Redis without waiting for
  the TTL.
- **Sliding-window rate limiting.** Currently there's a token bucket and
  `minTime`, but not a true sliding window log.
- **Job inspection.** Read a job's state without holding the closure.

## Reporting bugs

Open an issue with:

- What you expected and what happened instead.
- A minimal reproduction — the `examples/` folder has runnable snippets.
- Node version, Redis version, and whether you're on Redis Cluster.

If the bug is a race in the Lua, the output of `redis-cli MONITOR` around the
failure is worth more than anything else.

## Pull requests

- One logical change per PR. If you can split it, please do.
- Explain *why* in the description, not just *what*.
- If it changes Redis state or key layout, call that out explicitly — it
  affects anyone upgrading.

## Reporting security issues

Please don't open a public issue. See [SECURITY.md](SECURITY.md).

## Code of conduct

Be decent to each other. See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## License

MIT. By contributing you agree your work is licensed under it.
