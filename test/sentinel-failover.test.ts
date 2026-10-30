/**
 * test/sentinel-failover.test.ts
 *
 * The reason Sentinel exists: when the primary dies, the pool keeps working.
 *
 * This test kills the master outright and asserts that:
 *   1. the Sentinel promotes the replica
 *   2. BetterQueue reconnects to the new primary
 *   3. jobs enqueued AFTER the failover run normally
 *
 * The topology is disposable (master + replica + sentinel on fixed local
 * ports). A sentinel probe issues REPLICAOF and rewrites the master's role,
 * so it must never point at a production Redis.
 */
import { spawn, type ChildProcess } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { WorkerPool, DistributedLimiter } from '../src/index';

// 📌 These MUST differ from the ports in sentinel.test.ts. Vitest runs test
// files in parallel, and two topologies sharing a port fight over it — the
// second file's SHUTDOWN takes the first file's server down mid-test.
const MASTER_PORT = 6480;
const REPLICA_PORT = 6481;
const SENTINEL_PORT = 26479;
const REDIS_NAME = 'bqf';

const tmpDirs: string[] = [];
const servers: ChildProcess[] = [];

function startServer(conf: string, dir: string, extraArgs: string[] = []): ChildProcess {
  const confPath = join(dir, 'redis.conf');
  writeFileSync(confPath, conf);
  const child = spawn('redis-server', [confPath, ...extraArgs], { stdio: 'ignore' });
  servers.push(child);
  return child;
}

function newDir(prefix: string): string {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  tmpDirs.push(dir);
  return dir;
}

async function waitForPort(port: number, timeoutMs = 20_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let last = '';
  while (Date.now() < deadline) {
    const out = await new Promise<string>((resolve) => {
      const r = spawn('redis-cli', ['-p', String(port), 'PING']);
      let buf = '';
      r.stdout.on('data', (d) => (buf += d));
      r.stderr.on('data', (d) => (buf += d));
      r.on('close', () => resolve(buf));
      r.on('error', () => resolve(''));
    });
    last = out.trim();
    if (last.includes('PONG')) return;
    await new Promise((res) => setTimeout(res, 300));
  }
  throw new Error(`redis on port ${port} did not come up (last: ${last})`);
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
        `pid=$(ss -lntpH "sport = :${port}" 2>/dev/null | grep -oP 'pid=\K[0-9]+' | head -1); ` +
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

/** Poll until the sentinel reports a different master than the one we killed. */
async function waitForNewMaster(oldPort: number, timeoutMs = 30_000): Promise<string> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const addr = await redisCli([
      '-p', String(SENTINEL_PORT), 'sentinel', 'get-master-addr-by-name', REDIS_NAME,
    ]);
    const port = addr.trim().split('\n').pop()?.trim();
    if (port && port !== String(oldPort)) return port;
    await sleep(500);
  }
  throw new Error('sentinel did not promote a new master in time');
}

describe('Redis Sentinel failover', () => {
  let pool: WorkerPool;

  beforeAll(async () => {
    // Start from a clean slate — a leftover server on these ports would make
    // this test pass against the wrong topology.
    await freePorts([MASTER_PORT, REPLICA_PORT, SENTINEL_PORT]);

    const mdir = newDir('bq-fo-master-');
    startServer(
      `port ${MASTER_PORT}\ncluster-enabled no\nappendonly no\nsave ""\ndir ${mdir}\nbind 127.0.0.1\nprotected-mode no\n`,
      mdir,
      ['--daemonize', 'yes'],
    );
    await waitForPort(MASTER_PORT);

    const rdir = newDir('bq-fo-replica-');
    startServer(
      `port ${REPLICA_PORT}\ncluster-enabled no\nappendonly no\nsave ""\ndir ${rdir}\nbind 127.0.0.1\nprotected-mode no\n`,
      rdir,
      ['--daemonize', 'yes'],
    );
    await waitForPort(REPLICA_PORT);
    await redisCli(['-p', String(REPLICA_PORT), 'REPLICAOF', '127.0.0.1', String(MASTER_PORT)]);
    await sleep(3_000);

    const sdir = newDir('bq-fo-sentinel-');
    const sentinelConf = [
      `port ${SENTINEL_PORT}`,
      'daemonize yes',
      `dir ${sdir}`,
      'bind 127.0.0.1',
      'protected-mode no',
      // 📌 Quorum MUST be 1 here. A single sentinel cannot reach quorum 2, so
      // it would detect the failure and then never promote — the master would
      // stay down forever and the test would time out waiting for a
      // promotion that can never happen. Quorum 2 is correct in production
      // with three sentinels; it is wrong for a one-sentinel test.
      `sentinel monitor ${REDIS_NAME} 127.0.0.1 ${MASTER_PORT} 1`,
      `sentinel down-after-milliseconds ${REDIS_NAME} 2000`,
      `sentinel failover-timeout ${REDIS_NAME} 5000`,
      `sentinel parallel-syncs ${REDIS_NAME} 1`,
    ].join('\n');
    writeFileSync(join(sdir, 'sentinel.conf'), sentinelConf);
    const s = spawn('redis-server', [join(sdir, 'sentinel.conf'), '--sentinel'], { stdio: 'ignore' });
    servers.push(s);
    // The sentinel daemonises itself, so the spawned child exits immediately
    // while the daemon keeps running. Poll for the port rather than the pid.
    await waitForPort(SENTINEL_PORT);

    pool = await WorkerPool.create({
      redis: { sentinels: [{ host: '127.0.0.1', port: SENTINEL_PORT }], name: REDIS_NAME },
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
    for (const port of [MASTER_PORT, REPLICA_PORT, SENTINEL_PORT]) {
      await redisCli(['-p', String(port), 'SHUTDOWN', 'NOSAVE']);
    }
    for (const d of tmpDirs) {
      try {
        rmSync(d, { recursive: true, force: true });
      } catch {
        /* best effort */
      }
    }
  });

  it('keeps processing jobs after the primary is killed', async () => {
    const limiter = new DistributedLimiter({ id: 'failover:queue', maxConcurrent: 2 });

    // works before the failover
    const before = await limiter.schedule(async () => 'before-failover');
    expect(before).toBe('before-failover');

    // kill the primary outright
    await redisCli(['-p', String(MASTER_PORT), 'SHUTDOWN', 'NOSAVE']);

    // the sentinel promotes the replica
    const newMasterPort = await waitForNewMaster(MASTER_PORT);
    expect(newMasterPort).toBe(String(REPLICA_PORT));

    // give the client time to notice and reconnect
    await sleep(2_000);

    // and jobs keep running
    const after = await limiter.schedule(async () => 'after-failover');
    expect(after).toBe('after-failover');
  }, 90_000);
});
