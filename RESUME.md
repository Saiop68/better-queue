# BetterQueue promotion — resume checkpoint

Written 2026-10-30 ~04:30 UTC. If a session dies mid-task, read this first.

## Current state (verified)

| Thing | State |
|---|---|
| npm | `@destroyer777777/betterqueue@1.0.1` live |
| repo | `Saiop68/better-queue` public, MIT |
| CI | green on Node 18/20/22 + tarball job |
| tests | 37/37 |
| org | `Better-Queue` DELETED by user — do not recreate |
| GitHub Packages | scope `@better-queue` retired with the org |
| Discord refs | stripped from src/scripts/examples/docs/package.json |
| topics | 20 max — `bottleneck,distributed,ioredis,job-queue,queue,rate-limiter,redis,async,backpressure,bullmq,concurrency,crash-recovery,memory-efficient,nodejs,performance,rate-limiting,redis-queue,token-bucket,typescript` |
| repo description | "Redis-backed distributed rate limiter + job queue for Node.js. Zero per-limiter timers, cross-process priority, crash recovery. A modern Bottleneck alternative." |
| Discussions | enabled |
| vuln alerts | enabled |
| Bottleneck issue replies | 36/36 posted |

## Market intel (measured, not guessed)

```
bottleneck        47,660,899 downloads/month   <-- the real target
better-queue         635,972 downloads/month   <-- different package, by diamondio
```
`better-queue` on npm is `diamondio/better-queue` — a cargo-style queue with
`uuid`/`node-eta` deps. NOT ours, NOT the same thing.
`bottleneck` has ZERO dependencies and is only at 2.19.5, last commit 2020.

npm similarity policy blocks `betterqueue` (too close to `better-queue`).
Only the scoped name works.

## What I was doing

Benchmarking Bottleneck vs BetterQueue to produce hard numbers for the
landing page + README. `benchmark/compare.mjs` written at
`/root/.hermes/cache/scratch/bq-review/benchmark/compare.mjs`.

**Gotcha:** ts-node/typescript version mismatch in the bench dir —
`ts-node` crashes with `Cannot read properties of undefined (reading 'fileExists')`.
Fix: use plain ESM `.mjs` against `dist/index.mjs`, not ts-node.

## Next steps, in priority order

1. Run `benchmark/compare.mjs` → real numbers (memory delta, timer count, throughput)
2. Put results in README + a GitHub Pages landing site
3. Open honest migration PRs to repos that import bottleneck
   (GitHub code search found 115 TS hits; examples:
   marceltft/bitbucket-mcp, pawtograder/platform, perplext/coordinaitor)
4. PR to awesome-lists covering redis/queues
5. GitHub Sponsors profile

## Constraints I must respect

- **Do NOT use Xieron (the bot, the dashboard, the website) to promote this.**
  User was explicit. Promotion must live entirely in the library's own
  channels.
- Never touch MongoDB. No production service restarts without explicit OK.
- OPSEC: no IPs/paths/tokens in any public post or reply.

## Working dirs

```
/root/.hermes/cache/scratch/bq-review    <- the library, git repo, push target
/root/.hermes/cache/scratch/bq-bench     <- bench sandbox (bottleneck + bq installed)
/root/.hermes/cache/scratch/bq-logo      <- logo assets
/root/.hermes/cache/scratch/bq-issues    <- the 36 posted Bottleneck replies
```

## Skill

`library-publishing` skill exists at
`/root/.hermes/skills/software-development/library-publishing/SKILL.md`
with the full playbook (npm naming, org creation, avatar verification, CI
tarball gotcha, publish+verify loop, issue-reply rules).
