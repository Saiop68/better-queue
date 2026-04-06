// Check if LPUSH actually writes to the list
import { createRedisClient } from '../src/RedisClient';
import { KEYS } from '../src/constants';

async function main() {
  const r = createRedisClient({ host: '127.0.0.1', port: 6379 });
  
  // Check current state
  const before = await r.llen(KEYS.notify);
  console.log('LLEN before:', before);
  
  // Push something
  await r.lpush(KEYS.notify, 'queue-test-1');
  await r.lpush(KEYS.notify, 'queue-test-2');
  
  const after = await r.llen(KEYS.notify);
  console.log('LLEN after push:', after);
  
  // Pop it back
  const popped = await r.rpop(KEYS.notify);
  console.log('RPOP result:', popped);
  
  const final = await r.llen(KEYS.notify);
  console.log('LLEN final:', final);
  
  // Check what key the notify is
  console.log('Notify key:', KEYS.notify);
  
  await r.quit();
  process.exit(0);
}

main().catch(e => { console.error(e); process.exit(1); });
