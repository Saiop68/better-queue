// Ultra-minimal debug — check if BRPOP sees messages
import { createBlockingClient, createRedisClient } from '../src/RedisClient';
import { KEYS } from '../src/constants';

async function main() {
  const pub = createRedisClient({ host: '127.0.0.1', port: 6379 });
  const sub = createBlockingClient({ host: '127.0.0.1', port: 6379 });

  console.log('pub ping:', await pub.ping());
  console.log('sub ping:', await sub.ping());

  // Start BRPOP listener
  let received = false;
  const waiter = sub.brpop(KEYS.notify, 5).then((result) => {
    console.log('BRPOP result:', result);
    received = true;
    return result;
  });

  // Publish a message after a short delay
  await new Promise(r => setTimeout(r, 200));
  console.log('Publishing to', KEYS.notify);
  await pub.lpush(KEYS.notify, 'test-queue-id');
  console.log('Published');

  await waiter;
  console.log('Received:', received);

  await pub.quit();
  await sub.quit();
  process.exit(0);
}

main().catch(e => { console.error(e); process.exit(1); });
