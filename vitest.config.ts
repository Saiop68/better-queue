import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/**/*.test.ts'],
    // The Sentinel suites need the `redis-server` and `redis-cli` binaries
    // installed locally: they start a disposable master/replica/sentinel
    // topology and then kill it. The default CI job only has a Redis
    // *service container*, which ships no client binary, so those suites
    // would fail there with `spawn redis-cli ENOENT`.
    //
    // They are not skipped — they run in the dedicated `sentinel` CI job,
    // which installs redis-server. Excluding them here keeps the default
    // run meaningful for anyone who cloned the repo and ran `npm test`
    // without a local Redis toolchain.
    exclude: ['test/sentinel.test.ts', 'test/sentinel-failover.test.ts'],
    // The WorkerPool is a process-wide singleton and all suites share one
    // Redis instance, so files must not run in parallel.
    fileParallelism: false,
    testTimeout: 30_000,
    hookTimeout: 30_000,
    teardownTimeout: 15_000,
  },
});
