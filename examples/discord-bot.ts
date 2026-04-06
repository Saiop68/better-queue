/**
 * examples/discord-bot.ts — Real-world Discord bot usage pattern.
 *
 * Demonstrates per-guild rate limiting for Discord API calls.
 * Each guild gets its own DistributedLimiter sharing a Redis-backed
 * rate limit state — safe across multiple shards.
 *
 * Run: npx ts-node examples/discord-bot.ts
 */

import { WorkerPool, DistributedLimiter } from '../src';
import type { LimiterOptions } from '../src';

// ── Simulated Discord API ──────────────────────────────────────────────────

interface DiscordMessage {
  guildId: string;
  channelId: string;
  content: string;
}

async function sendDiscordMessage(msg: DiscordMessage): Promise<{ id: string }> {
  // Simulate Discord API latency (20–80ms)
  await new Promise((r) => setTimeout(r, 20 + Math.random() * 60));

  const msgId = Math.random().toString(36).slice(2, 12);
  console.log(`  📨 [${msg.guildId}] Sent: "${msg.content.slice(0, 30)}" → id=${msgId}`);
  return { id: msgId };
}

async function banMember(guildId: string, userId: string): Promise<void> {
  await new Promise((r) => setTimeout(r, 30 + Math.random() * 50));
  console.log(`  🔨 [${guildId}] Banned user ${userId}`);
}

// ── Limiter factory (cache limiters per guild) ─────────────────────────────

const limiterCache = new Map<string, DistributedLimiter>();

function getGuildLimiter(guildId: string, opts?: Partial<LimiterOptions>): DistributedLimiter {
  const cached = limiterCache.get(guildId);
  if (cached) return cached;

  const limiter = new DistributedLimiter({
    id: `guild:${guildId}`,
    maxConcurrent: 10,       // Discord allows bursting
    minTime: 100,           // 100ms between job starts (~10 req/s sustained)
    reservoir: 50,          // burst budget
    reservoirRefreshInterval: 1_000,
    reservoirRefreshAmount: 10,  // +10 tokens/second
    priority: 5,
    ttl: 3_600,             // 1 hour TTL — inactive guilds auto-expire
    ...opts,
  });

  // Only store weak reference concept — cache is bounded by eviction in real bots
  limiterCache.set(guildId, limiter);

  // Forward pool events for observability
  limiter.on('failed', (jobId, err) => {
    console.error(`  ❌ [${guildId}] Job ${jobId} permanently failed:`, err);
  });

  limiter.on('retry', (jobId, attempt) => {
    console.log(`  🔄 [${guildId}] Retrying job ${jobId} (attempt ${attempt})`);
  });

  return limiter;
}

// ── Simulate 10k-guild Discord bot ────────────────────────────────────────

async function simulateAntiNukeResponse(guildId: string, attackerIds: string[]): Promise<void> {
  const limiter = getGuildLimiter(guildId, {
    maxConcurrent: 10,
    minTime: 50,
    priority: 1, // high-priority queue for security actions
  });

  console.log(`\n🔒 [${guildId}] Anti-nuke triggered — banning ${attackerIds.length} members`);

  const banPromises = attackerIds.map((userId) =>
    limiter.schedule(() => banMember(guildId, userId), { priority: 1 }),
  );

  await Promise.all(banPromises);
  console.log(`✅ [${guildId}] Anti-nuke complete\n`);
}

async function simulateModBotMessages(guildId: string, count: number): Promise<void> {
  const limiter = getGuildLimiter(guildId);

  const messages = Array.from({ length: count }, (_, i) => ({
    guildId,
    channelId: 'mod-log',
    content: `Mod action #${i + 1} logged at ${new Date().toISOString()}`,
  }));

  await Promise.all(
    messages.map((msg) => limiter.schedule(() => sendDiscordMessage(msg))),
  );
}

// ── Main ───────────────────────────────────────────────────────────────────

async function main() {
  console.log('🤖 BetterQueue — Discord Bot Example\n');
  console.log('Simulating a bot across 10 guilds with concurrent API calls…\n');

  const pool = await WorkerPool.create({
    redis: { host: '127.0.0.1', port: 6379 },
    workers: 8, // 8 workers for high throughput
    tickIntervalMs: 25, // tight tick for low-latency rate limiting
  });

  const guildIds = Array.from({ length: 10 }, (_, i) => `guild_${10 + i}`);

  // Simulate concurrent activity across all guilds
  const tasks: Promise<void>[] = [];

  for (const guildId of guildIds) {
    // Each guild sends 5 messages
    tasks.push(simulateModBotMessages(guildId, 5));
  }

  // One guild triggers an anti-nuke (high priority)
  tasks.push(simulateAntiNukeResponse('guild_105', ['user_1', 'user_2', 'user_3']));

  const start = Date.now();
  await Promise.all(tasks);
  console.log(`\n⏱ All tasks completed in ${Date.now() - start}ms`);

  // Print stats for a couple of guilds
  for (const guildId of guildIds.slice(0, 3)) {
    const limiter = getGuildLimiter(guildId);
    const stats = await limiter.getStats();
    console.log(`📊 [${guildId}] Stats:`, stats);
  }

  // Demonstrate drain()
  console.log('\nDraining all limiters…');
  await Promise.all([...limiterCache.values()].map((l) => l.drain()));
  console.log('All queues drained.\n');

  await pool.stop();
  console.log('✅ Pool stopped. Process will exit cleanly.');
}

main().catch(console.error);
