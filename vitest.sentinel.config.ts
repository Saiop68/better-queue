import { defineConfig } from 'vitest/config';

/**
 * Config for the Redis Sentinel suites.
 *
 * These are kept out of the default `vitest.config.ts` run on purpose: they
 * need the `redis-server` and `redis-cli` binaries installed locally, because
 * they start a disposable master/replica/sentinel topology and then kill it
 * to force a failover. The default CI job has a Redis *service container*,
 * which ships no client binary, so those suites would fail there with
 * `spawn redis-cli ENOENT`.
 *
 * Run them with:
 *   npx vitest run --config vitest.sentinel.config.ts
 * or:
 *   npm run test:sentinel
 */
export default defineConfig({
  test: {
    include: ['test/sentinel.test.ts', 'test/sentinel-failover.test.ts'],
    // The WorkerPool is a process-wide singleton and both suites build their
    // own topology, so files must not run in parallel.
    fileParallelism: false,
    testTimeout: 60_000,
    hookTimeout: 60_000,
    teardownTimeout: 30_000,
  },
});
