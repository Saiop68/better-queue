/**
 * test/sentinel.test.ts
 *
 * Verifies that BetterQueue works when the Redis primary is discovered
 * through a Sentinel, including the failover path.
 *
 * The Sentinel topology is created by the test itself (master + replica +
 * sentinel on disposable ports) so the suite never touches a production
 * Redis. A sentinel probe issues REPLICAOF against its target and rewrites
 * the master's role, so it MUST run against disposable processes.
 */
import { spawn, type ChildProcess } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { WorkerPool, DistributedLimiter, limiterManager } from '../src/index';

// 📌 These MUST differ from the ports in sentinel-failover.test.ts. Vitest
// runs test files in parallel and two topologies sharing a port fight over it.
const MASTER_PORT = 6380;
const REPLICA_PORT = 6381;
const SENTINEL_PORT = 26379;
const REDIS_NAME = 'bqt';

const sentinels = [{ host: '127.0.0.1', port: SENTINEL_PORT }];

let tmpDirs: string[] = [];
const servers: ChildProcess[] = [];

function redisConf(port: number, dir: string, extra = ''): string {
  return [
    `port ${port}`,
    'cluster-enabled no',
    'appendonly no',
    'save ""',
    `dir ${dir}`,
    'bind 127.0.0.1',
    'protected-mode no',
    extra,
  ].join('\n');
}

function startServer(conf: string, extraArgs: string[] = []): ChildProcess {
  const dir = mkdtempSync(join(tmpdir(), 'bq-sentinel-'));
  tmpDirs.push(dir);
  const confPath = join(dir, 'redis.conf');
  const { writeFileSync } = require('node:fs') as typeof import('node:fs');
  writeFileSync(confPath, conf.replace('__DIR__', dir));
  const child = spawn('redis-server', [confPath, ...extraArgs], {
    stdio: 'ignore',
    detached: false,
  });
  servers.push(child);
  return child;
}

async function waitForPort(port: number, timeoutMs = 15_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const r = spawn('redis-cli', ['-p', String(port), 'PING']);
    const out = await new Promise<string>((resolve) => {
      let buf = '';
      r.stdout.on('data', (d) => (buf += d));
      r.on('close', () => resolve(buf));
    });
    if (out.includes('PONG')) return;
    await new Promise((res) => setTimeout(res, 250));
  }
  throw new Error(`redis on port ${port} did not come up`);
}

function redisCli(args: string[]): Promise<string> {
  return new Promise((resolve, reject) => {
    const r = spawn('redis-cli', args);
    let buf = '';
    r.stdout.on('data', (d) => (buf += d));
    r.stderr.on('data', (d) => (buf += d));
    r.on('close', () => resolve(buf));
    r.on('error', reject);
  });
}


async function sleep(ms: number): Promise<void> {
  await new Promise((r) => setTimeout(r, ms));
}

/**
 * Free the given ports.
 *
 * `redis-cli SHUTDOWN` only works against a live Redis, and a daemonised
 * server survives a SHUTDOWN aimed at a different instance. Find the pid
 * listening on the port and kill it, so a leftover server can never make this
 * test run against the wrong topology.
 */
async function freePorts(ports: number[]): Promise<void> {
  for (const port of ports) {
    await redisCli(['-p', String(port), 'SHUTDOWN', 'NOSAVE']);
    const killed = await new Promise<string>((resolve) => {
      const r = spawn('bash', [
        '-c',
        `pid=$(ss -lntpH "sport = :${port}" 2>/dev/null | grep -oP 'pid=\\K[0-9]+' | head -1); ` +
        `if [ -n "$pid" ]; then kill -9 "$pid" 2>/dev/null && echo "killed $pid"; else echo none; fi`,
      ]);
      let buf = '';
      r.stdout.on('data', (d) => (buf += d));
      r.on('close', () => resolve(buf.trim()));
    });
    void killed;
  }
  await sleep(500);
}

describe('Redis Sentinel', () => {
  let pool: WorkerPool;

  beforeAll(async () => {
    // A leftover server on these ports would answer PING and make this test
    // run against the wrong topology.
    await freePorts([MASTER_PORT, REPLICA_PORT, SENTINEL_PORT]);

    // master
    startServer(redisConf(MASTER_PORT, '__DIR__'), ['--daemonize', 'yes']);
    await waitForPort(MASTER_PORT);

    // replica
    startServer(redisConf(REPLICA_PORT, '__DIR__'), ['--daemonize', 'yes']);
    await waitForPort(REPLICA_PORT);
    await redisCli(['-p', String(REPLICA_PORT), 'REPLICAOF', '127.0.0.1', String(MASTER_PORT)]);
    await new Promise((r) => setTimeout(r, 3_000));

    // sentinel
    const sentinelConf = [
      `port ${SENTINEL_PORT}`,
      'daemonize yes',
      'bind 127.0.0.1',
      'protected-mode no',
      // 📌 Quorum 1: a single sentinel can never reach quorum 2. See the note
      // in sentinel-failover.test.ts.
      `sentinel monitor ${REDIS_NAME} 127.0.0.1 ${MASTER_PORT} 1`,
      `sentinel down-after-milliseconds ${REDIS_NAME} 2000`,
      `sentinel failover-timeout ${REDIS_NAME} 5000`,
      `sentinel parallel-syncs ${REDIS_NAME} 1`,
    ].join('\n');
    const sdir = mkdtempSync(join(tmpdir(), 'bq-sentinel-s-'));
    tmpDirs.push(sdir);
    const { writeFileSync } = require('node:fs') as typeof import('node:fs');
    writeFileSync(join(sdir, 'sentinel.conf'), sentinelConf.replace('__DIR__', sdir));
    writeFileSync(join(sdir, 'sentinel.conf'), sentinelConf);
    const s = spawn('redis-server', [join(sdir, 'sentinel.conf'), '--sentinel'], {
      stdio: 'ignore',
    });
    servers.push(s);
    await waitForPort(SENTINEL_PORT);

    // sanity: sentinel knows the master
    const addr = await redisCli(['-p', String(SENTINEL_PORT), 'sentinel', 'get-master-addr-by-name', REDIS_NAME]);
    expect(addr).toContain(String(MASTER_PORT));

    pool = await WorkerPool.create({
      redis: { sentinels, name: REDIS_NAME },
      workers: 2,
    });
  }, 60_000);

  afterAll(async () => {
    if (pool) await pool.stop();
    for (const s of servers) {
      try {
        s.kill('SIGKILL');
      } catch {
        /* already gone */
      }
    }
    await redisCli(['-p', String(MASTER_PORT), 'SHUTDOWN', 'NOSAVE']);
    await redisCli(['-p', String(REPLICA_PORT), 'SHUTDOWN', 'NOSAVE']);
    await redisCli(['-p', String(SENTINEL_PORT), 'SHUTDOWN', 'NOSAVE']);
    for (const d of tmpDirs) {
      try {
        rmSync(d, { recursive: true, force: true });
      } catch {
        /* best effort */
      }
    }
  });

  it('runs a job through the Sentinel-discovered primary', async () => {
    const limiter = new DistributedLimiter({ id: 'sentinel:job', maxConcurrent: 2 });
    const result = await limiter.schedule(async () => 'ran-through-sentinel');
    expect(result).toBe('ran-through-sentinel');
  });

  it('enqueues and drains several jobs', async () => {
    const limiter = new DistributedLimiter({ id: 'sentinel:many', maxConcurrent: 2 });
    const results = await Promise.all(
      Array.from({ length: 10 }, (_, i) => limiter.schedule(async () => i)),
    );
    expect(results).toHaveLength(10);
    expect(results.sort((a, b) => a - b)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
  });

  it('keeps queue state on the primary', async () => {
    const limiter = new DistributedLimiter({ id: 'sentinel:state', maxConcurrent: 1 });
    await limiter.schedule(async () => 'x');
    const stats = await limiter.getStats();
    expect(stats).toBeDefined();
    expect(typeof stats.queueSize).toBe('number');
    // the queue must exist on the master, not on the sentinel
    const keys = await redisCli(['-p', String(MASTER_PORT), 'KEYS', 'bq:sentinel:state:*']);
    expect(keys).toContain('bq:sentinel:state:');
  });

  it('exposes the pool lifecycle through the Sentinel', async () => {
    // the pool registers itself in bq:pools on the primary
    const pools = await redisCli(['-p', String(MASTER_PORT), 'ZRANGE', 'bq:pools', '0', '-1']);
    expect(pools.split('\n').filter(Boolean).length).toBeGreaterThan(0);
  });
});
