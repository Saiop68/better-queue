# BetterQueue promotion — resume checkpoint

Written 2026-10-30 ~04:30 UTC. If a session dies mid-task, read this first.

## Current state (verified 2026-10-30)

| Thing | State |
|---|---|
| npm | `@destroyer777777/betterqueue@1.2.0` live |
| npm README | 59,893 chars — fits npm's 64 KB cap, Contributing + License visible |
| repo | `Saiop68/better-queue` public, MIT |
| releases | v1.0.0, v1.0.1, v1.1.0, v1.2.0 |
| CI | green on Node 18/20/22 + tarball job |
| tests | 37/37 |
| landing page | https://saiop68.github.io/better-queue/ (GitHub Pages, docs/) |
| org | `Better-Queue` DELETED by user — do not recreate |
| GitHub Packages | scope `@better-queue` retired with the org |
| Discord refs | stripped from src/scripts/examples/docs/package.json |
| topics | 20 max, incl. bottleneck, backpressure, bullmq, redis-queue, token-bucket |
| Discussions | enabled |
| vuln alerts | enabled |
| Bottleneck issue replies | 36/36 posted (0 reactions so far) |
| open PRs | 5 awesome-lists (see below) |
| npm keywords | 30 — rate-limiter, throttle, bottleneck, job-queue, redis, backpressure, bullmq, bull... |

## Open PRs

```
https://github.com/JamzyWang/awesome-redis/pull/38          (745★)
https://github.com/sorrycc/awesome-javascript/pull/1191     (35K★)
https://github.com/jobbole/awesome-javascript-cn/pull/45    (7.8K★)
https://github.com/gamedilong/awesome-nodejs-cn/pull/4      (360★)
https://github.com/Pines-Cheng/awesome-nodejs-cn/pull/3     (234★)
```

All open, mergeable, 1-file diffs. `sindresorhus/awesome-nodejs` (67K★) was
SKIPPED — their contributing rules require 100+ stars and 30 days old, which
the repo does not have yet.

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

1. **Stars are the bottleneck.** `sindresorhus/awesome-nodejs` (67K★) and
   similar lists require 100+ stars + 30 days age. Nothing else unlocks that
   traffic until the repo passes the bar.
2. GitHub Sponsors profile (free, adds a Sponsor button + funded-by badge).
3. Comment on real maintainer issues in active repos where our library
   genuinely solves the stated problem — bull/bullmq issue threads about
   stalled jobs, worker close hangs, Redis Cluster. NEVER the AI-generated
   roadmap issues; commenting there is spam.
4. More awesome-lists that do NOT have a star requirement.

## Key findings worth keeping

- npm search is **popularity-driven** — 30 keywords and a good description do
  NOT get you into the top 30 for "rate limiter" when the incumbent has 47M
  downloads/month. It only matters once there is download volume.
- npm truncates a package README at **65,536 chars**. Over that, the tail
  sections silently disappear from the npm page. Check
  `registry.npmjs.org/<scope>%2F<name>` → `.readme.length` after publishing.
- Bottleneck 2.19.5 does **NOT** create a timer per limiter unless a
  reservoir is configured (`LocalDatastore._startHeartbeat()` is conditional).
  The README originally claimed otherwise; it was corrected.
- `process.getActiveResourcesInfo()` is the authoritative live-timer count —
  monkey-patching `setInterval` misses `setTimeout`, and ESM hoists static
  imports so the patch must be installed BEFORE the library is imported
  (use dynamic `import()`).
- Benchmark each library in its **own process** or the second inherits the
  first one's heap.

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
