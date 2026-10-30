/**
 * examples/scope-member-add.ts
 *
 * Realistic simulation of a per-scope member-add queue using BetterQueue's LimiterManager.
 * The code structure mirrors a typical production handler 1:1 — only imports changed.
 *
 * MIGRATION from Bottleneck:
 *   Before:  import limiterManager from '../../Structures/LimiterManager.js';
 *   After:   import { limiterManager } from '@destroyer777777/betterqueue';
 *
 * Everything else — registerFeatureConfig, getLimiter, schedule({ id }, fn) — stays identical.
 *
 * Run (with GC exposure):
 *   node --expose-gc -r ts-node/register --project tsconfig.dev.json examples/scope-member-add.ts
 *   OR:
 *   npx ts-node --project tsconfig.dev.json --node-flags --expose-gc examples/scope-member-add.ts
 */

import { PerformanceObserver, performance } from 'node:perf_hooks';
import { WorkerPool, limiterManager } from '../src';

// ── Simulated domain types ───────────────────────────────────────────────

interface Role   { id: string; name: string; position: number; }
interface Member { id: string; username: string; bot: boolean; scopeId: string; }

// ── Memory + GC Profiler ──────────────────────────────────────────────────

interface MemSnapshot {
  label:      string;
  rss:        number;   // bytes — total process RSS
  heapUsed:   number;   // bytes — V8 live heap
  heapTotal:  number;   // bytes — V8 reserved heap
  external:   number;   // bytes — C++ objects (Buffers, ioredis internals)
  arrayBufs:  number;   // bytes — ArrayBuffer allocations
  at:         number;   // ms since process start
}

interface GcEvent {
  kind:     string;  // 'major' | 'minor' | 'incremental' | 'process_exit'
  pauseMs:  number;
  at:       number;
}

class MemProfiler {
  private snapshots: MemSnapshot[] = [];
  private gcEvents:  GcEvent[]     = [];
  private sampler:   ReturnType<typeof setInterval> | null = null;
  private observer:  PerformanceObserver | null = null;
  private peakHeap = 0;

  start(): void {
    // ── GC event observer (works without --expose-gc) ──────────────────────
    try {
      this.observer = new PerformanceObserver((list) => {
        for (const entry of list.getEntries()) {
          const kind = this.gcKind((entry as any).detail?.kind ?? -1);
          this.gcEvents.push({ kind, pauseMs: entry.duration, at: performance.now() });
        }
      });
      this.observer.observe({ entryTypes: ['gc'] });
    } catch {
      // Node versions < 16 may not support GC entries — silent fallback
    }

    // ── Periodic heap sampler (every 200ms) ────────────────────────────────
    this.sampler = setInterval(() => {
      const m = process.memoryUsage();
      if (m.heapUsed > this.peakHeap) this.peakHeap = m.heapUsed;
    }, 200);
    (this.sampler as any).unref?.();
  }

  snap(label: string): void {
    const m = process.memoryUsage();
    if (m.heapUsed > this.peakHeap) this.peakHeap = m.heapUsed;
    this.snapshots.push({
      label,
      rss:       m.rss,
      heapUsed:  m.heapUsed,
      heapTotal: m.heapTotal,
      external:  m.external,
      arrayBufs: m.arrayBuffers,
      at:        performance.now(),
    });
  }

  manualGc(label: string): void {
    // Only available when Node is launched with --expose-gc
    if (typeof (global as any).gc === 'function') {
      (global as any).gc();
      this.snap(`${label} [post-GC]`);
    }
  }

  stop(): void {
    if (this.sampler) { clearInterval(this.sampler); this.sampler = null; }
    if (this.observer) { this.observer.disconnect(); this.observer = null; }
  }

  report(): void {
    const mb = (b: number) => (b / 1_048_576).toFixed(2) + ' MB';
    const ms = (n: number) => n.toFixed(2) + ' ms';

    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');
    console.log('📈  MEMORY SNAPSHOTS\n');

    const labelW = Math.max(...this.snapshots.map(s => s.label.length), 5);
    const header =
      'Label'.padEnd(labelW) + '  ' +
      'RSS'.padStart(10)     + '  ' +
      'HeapUsed'.padStart(10) + '  ' +
      'HeapTotal'.padStart(10) + '  ' +
      'External'.padStart(10) + '  ' +
      'ArrBufs'.padStart(10);
    console.log(header);
    console.log('─'.repeat(header.length));

    for (const s of this.snapshots) {
      console.log(
        s.label.padEnd(labelW)       + '  ' +
        mb(s.rss).padStart(10)       + '  ' +
        mb(s.heapUsed).padStart(10)  + '  ' +
        mb(s.heapTotal).padStart(10) + '  ' +
        mb(s.external).padStart(10)  + '  ' +
        mb(s.arrayBufs).padStart(10),
      );
    }

    console.log('\n📌  PEAK heap (sampled every 200 ms): ' + mb(this.peakHeap));

    // Delta between first and last snapshot
    const first = this.snapshots[0];
    const last  = this.snapshots[this.snapshots.length - 1];
    if (first !== undefined && last !== undefined) {
      const deltaRss  = last.rss      - first.rss;
      const deltaHeap = last.heapUsed - first.heapUsed;
      console.log('📌  Net RSS  growth (start → end): ' + (deltaRss  >= 0 ? '+' : '') + mb(deltaRss));
      console.log('📌  Net heap growth (start → end): ' + (deltaHeap >= 0 ? '+' : '') + mb(deltaHeap));
    }

    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');

    if (this.gcEvents.length === 0) {
      console.log('🗑️   GC EVENTS — none observed  (run with --expose-gc for richer data)\n');
    } else {
      console.log(`🗑️   GC EVENTS  (${this.gcEvents.length} total)\n`);

      const byKind = new Map<string, { count: number; totalMs: number; maxMs: number }>();
      for (const ev of this.gcEvents) {
        const cur = byKind.get(ev.kind) ?? { count: 0, totalMs: 0, maxMs: 0 };
        cur.count++;
        cur.totalMs += ev.pauseMs;
        if (ev.pauseMs > cur.maxMs) cur.maxMs = ev.pauseMs;
        byKind.set(ev.kind, cur);
      }

      const gcHeader = 'Kind'.padEnd(14) + '  ' + 'Count'.padStart(6) + '  ' + 'Total pause'.padStart(14) + '  ' + 'Max pause'.padStart(12) + '  ' + 'Avg pause'.padStart(12);
      console.log(gcHeader);
      console.log('─'.repeat(gcHeader.length));
      for (const [kind, stat] of byKind) {
        console.log(
          kind.padEnd(14)                                + '  ' +
          String(stat.count).padStart(6)                + '  ' +
          ms(stat.totalMs).padStart(14)                 + '  ' +
          ms(stat.maxMs).padStart(12)                   + '  ' +
          ms(stat.totalMs / stat.count).padStart(12),
        );
      }

      const totalPause = this.gcEvents.reduce((a, e) => a + e.pauseMs, 0);
      console.log('\n📌  Total GC pause time: ' + ms(totalPause));

      // Longest single GC pause
      const worst = this.gcEvents.reduce((a, b) => b.pauseMs > a.pauseMs ? b : a);
      console.log('📌  Worst single pause:  ' + ms(worst.pauseMs) + '  (' + worst.kind + ')');
    }

    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');
  }

  private gcKind(k: number): string {
    // https://nodejs.org/api/perf_hooks.html#performanceentrykind
    switch (k) {
      case 1:  return 'major';
      case 2:  return 'minor';
      case 4:  return 'incremental';
      case 8:  return 'process_exit';
      default: return `unknown(${k})`;
    }
  }
}

// ── Simulated DB configs ───────────────────────────────────────────────────

function fakeAutorole(scopeId: string) {
  return { scopeId, roles: { humans: ['role-member'], bots: ['role-bot'] } };
}

function fakeWelcomeConfig(scopeId: string) {
  return { scopeId, presets: [{ channelId: 'ch-welcome', type: 'text', message: 'Welcome {username}!' }] };
}

// ── ScopeMemberAddQueue (your exact pattern) ─────────────────────────────

class ScopeMemberAddQueue {
  constructor() {
    // ← This is the ONLY line that changed from Bottleneck
    limiterManager.registerFeatureConfig('scope-member-add', {
      maxConcurrent: 100,
      reservoir: 10000,
      reservoirRefreshAmount: 10000,
      reservoirRefreshInterval: 10_000,
      minTime: 1_000,
      timeout: 3_600_000,
      trackDoneStatus: false,   // compat shim — ignored by BetterQueue
    });
  }

  addToQueue(scopeId: string, member: Member): void {
    // Exact same pattern as your bot:
    const limiter = limiterManager.getLimiter(scopeId, 'scope-member-add');
    const uniqueId = `${scopeId}-${member.id}-${Date.now()}`;

    limiter.schedule({ id: uniqueId }, async () => {
      try {
        // — Autorole —
        const autorole = fakeAutorole(scopeId);
        const type = member.bot ? 'bots' : 'humans';
        const roleIds = autorole.roles[type] ?? [];
        console.log(`  [${scopeId}] Assigning roles to ${member.username}: [${roleIds.join(', ')}]`);
        await new Promise(r => setTimeout(r, 20)); // simulate a slow API call

        // — Welcome message —
        const welcome = fakeWelcomeConfig(scopeId);
        for (const preset of welcome.presets) {
          const msg = preset.message.replace('{username}', member.username);
          console.log(`  [${scopeId}] Sending welcome to #${preset.channelId}: "${msg}"`);
          await new Promise(r => setTimeout(r, 10));
        }
      } catch (err) {
        console.error(`  [${scopeId}] Error in ScopeMemberAdd for ${member.username}:`, err);
      }
    }).catch(() => { /* limiter stopped / timed out — safe to ignore */ });
  }
}

// ── Main ──────────────────────────────────────────────────────────────────

async function main() {
  const mem = new MemProfiler();
  mem.start();
  mem.snap('process start');

  console.log('🤖 ScopeMemberAddQueue — BetterQueue LimiterManager demo\n');

  await WorkerPool.create({
    redis: { host: '127.0.0.1', port: 6379 },
    workers: 100,
    tickIntervalMs: 50,
  });

  mem.snap('pool ready (100 workers)');

  const queue = new ScopeMemberAddQueue();

  const SCOPES = ['scope_a'];
  const MEMBERS = 100;

  console.log(`Simulating ${SCOPES.length} scopes × ${MEMBERS} joins each...\n`);

  const start = Date.now();

  // Simulate members joining each scope concurrently
  const done = SCOPES.flatMap((scopeId, si) =>
    Array.from({ length: MEMBERS }, (_, mi) => {
      const member: Member = {
        id:       `user-${gi}-${mi}`,
        username: `Player${gi}${mi}`,
        bot:      mi === 2, // every third join is a bot
        scopeId,
      };

      return new Promise<void>((resolve) => {
        setTimeout(() => {
          queue.addToQueue(scopeId, member);
          resolve();
        }, gi * 50);
      });
    }),
  );

  await Promise.all(done);
  mem.snap(`all ${SCOPES.length * MEMBERS} jobs enqueued`);

  // Wait for all queues to drain (all API calls finished)
  console.log('\nWaiting for all queues to drain...');

  const drains = SCOPES.map(scopeId =>
    limiterManager.drainScope(scopeId, 'scope-member-add')
  );
  await Promise.all(drains);

  const elapsed = Date.now() - start;
  console.log(`\n✅ All ${SCOPES.length * MEMBERS} member joins processed in ${elapsed}ms`);

  mem.snap('all queues drained');
  mem.manualGc('post-drain');          // forces a GC cycle if --expose-gc is set
  mem.snap('post-drain GC settled');

  // Print cache stats (same format as original LimiterManager)
  console.log('\n📊 LimiterManager stats:');
  console.log(JSON.stringify(limiterManager.getStats(), null, 2));
  console.log('\n🩺 Cache health:');
  console.log(JSON.stringify(limiterManager.getCacheUsage(), null, 2));

  const pool = WorkerPool.getInstance();
  await pool.stop();

  mem.snap('pool stopped');
  mem.manualGc('post-stop');           // one more GC to see what Redis connections freed
  mem.snap('post-stop GC settled');

  mem.stop();
  mem.report();

  console.log('✅ Pool stopped. Process will exit cleanly.');
  process.exit(0);
}

main().catch(e => { console.error(e); process.exit(1); });
