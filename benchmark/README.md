# Benchmarks

`compare.mjs` runs Bottleneck 2.19.5 and BetterQueue against the same Redis
and prints the numbers. Each library runs in its own process — running both in
one process contaminates the second measurement with the first library's heap.

```bash
npm install bottleneck --no-save
npm run build

REDIS_PORT=6379 node --expose-gc benchmark/compare.mjs bottleneck
REDIS_PORT=6379 node --expose-gc benchmark/compare.mjs betterqueue
```

The script prints a `JSON {...}` line at the end, which is the machine-readable
form of the table above it.

## Reading the results

- **RSS** is the number that matters for a process running many limiters.
  Bottleneck holds its queue state in-process; BetterQueue holds it in Redis,
  so the process only keeps a thin handle per limiter.
- **heap after forced GC** is the retained memory once garbage has been
  collected. This is the honest comparison — the un-GC'd number mostly
  reflects allocation churn.
- **throughput** is jobs per second across the whole fleet.

## A note on timers

The script reports live timers read from `process.getActiveResourcesInfo()`.
Bottleneck 2.19.5 creates its per-limiter timers lazily, so for the
configurations measured here the number is zero for both libraries. Earlier
versions of this benchmark claimed Bottleneck creates one timer per limiter;
that claim was wrong and the measurement does not support it.

**Only quote figures this script actually prints.**
