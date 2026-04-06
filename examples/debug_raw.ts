// Raw ioredis test — no custom commands
import Redis from 'ioredis';

async function main() {
  const r = new Redis({ host: '127.0.0.1', port: 6379 });
  
  console.log('Connected. PING:', await r.ping());
  console.log('DBSIZE:', await r.dbsize());
  
  // Raw LPUSH
  const pushResult = await r.lpush('bq:workers:notify', 'hello');
  console.log('LPUSH result (should be 1+):', pushResult);
  
  const llen = await r.llen('bq:workers:notify');
  console.log('LLEN bq:workers:notify:', llen);
  
  // Immediate RPOP
  const rpop = await r.rpop('bq:workers:notify');
  console.log('RPOP:', rpop);
  
  // BRPOP test with immediate push
  const brpopPromise = r.brpop('bq:workers:notify', 3);
  
  // Push after 100ms using a separate connection
  const r2 = new Redis({ host: '127.0.0.1', port: 6379 });
  setTimeout(async () => {
    const res = await r2.lpush('bq:workers:notify', 'from-r2');
    console.log('r2 LPUSH result:', res);
  }, 100);
  
  const brpopResult = await brpopPromise;
  console.log('BRPOP result:', brpopResult);
  
  await r.quit();
  await r2.quit();
  process.exit(0);
}

main().catch(e => { console.error(e.message, e.stack); process.exit(1); });
