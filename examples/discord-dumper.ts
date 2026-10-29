/**
 * examples/discord-dumper.ts
 *
 * Instructions:
 * run: npx ts-node --project tsconfig.dev.json examples/discord-dumper.ts
 *
 * NOTE: this example needs `discord.js`, which is a devDependency of this
 * repo (NOT a runtime dependency of the library):
 *   npm install discord.js
 *
 * Once the bot is online, go to your Discord server and type "!spam"
 * to instantly queue 100 messages across every single channel.
 */

import { Client, GatewayIntentBits, Message } from 'discord.js';
import { WorkerPool, limiterManager } from '../src';

const BOT_TOKEN = process.env['DISCORD_BOT_TOKEN'] ?? '';

// ── 1. Configure the Limiter Rules ────────────────────────────────────────

// We register our rule profile. 
// We will apply this rule PER CHANNEL ID down below.
limiterManager.registerFeatureConfig('channel-spam', {
  maxConcurrent: 100,         // Allow bursting up to 3 messages instantly per channel
  minTime: 5000,            // But mathematically enforce a 1000ms minimum gap overall
});

const client = new Client({ 
  intents: [
    GatewayIntentBits.Guilds, 
    GatewayIntentBits.GuildMessages, 
    GatewayIntentBits.MessageContent 
  ] 
});

// ── 2. Run the Test ───────────────────────────────────────────────────────

client.once('ready', async () => {
  console.log(`🤖 Logged in as ${client.user?.tag}!`);

  // Start the queue engine - we configure 100 global connections to Redis
  // Meaning the bot can process up to 100 different channels simultaneously.
  await WorkerPool.create({
    redis: { host: '127.0.0.1', port: 6379 },
    workers: 1000, 
  });

  console.log(`✅ BetterQueue WorkerPool online with 100 concurrent workers.`);
  console.log(`👉 Type "!spam" in any text channel in your server to start the mass firehose!`);
});

client.on('messageCreate', async (message: Message) => {
  if (message.author.bot || message.content !== '!spam') return;
  if (!message.guild) return;

  console.log(`\n🔥 Booting up the Mass Dumper for Guild: ${message.guild.name}...`);
  
  // 1. Fetch all channels in the server
  const channels = await message.guild.channels.fetch();
  let textChannelsCount = 0;
  
  for (const [, channel] of channels) {
    if (!channel || !channel.isTextBased()) continue;
    textChannelsCount++;

    // 2. THIS IS THE SECRET SAUCE:
    // We get the limiter using the CHANNEL ID, not the Guild ID.
    // This perfectly isolates the queue to run 1 queue per specific channel.
    const limiter = limiterManager.getLimiter(channel.id, 'channel-spam');
    const channelName = channel.name;

    // 3. Queue 100 messages for this specific channel
    for (let i = 1; i <= 100; i++) {
       const jobId = `spam-${channel.id}-${i}`;

       limiter.schedule({ id: jobId }, async () => {
         const timestamp = new Date().toISOString().split('T')[1]?.slice(0, 8) ?? '00:00:00'; // hh:mm:ss
         try {
           console.log(`[${timestamp}] 🚀 [${channelName}] Firing Job #${i}...`);
           await channel.send(`Load Test - Channel: \`${channelName}\` | Message: **#${i}**`);
         } catch (err) {
           console.error(`❌ [${channelName}] Job #${i} failed:`, (err as Error).message);
         }
       }).catch(() => { /* handle limiter/queue ignores silently */});
    }
  }

  const totalJobs = 100 * textChannelsCount;
  console.log(`✅ Instantly queued ${totalJobs} messages across ${textChannelsCount} channels!`);
  console.log(`⏳ BetterQueue is now silently trickling them out in parallel based on the limiter rules.`);
});

client.login(BOT_TOKEN);
