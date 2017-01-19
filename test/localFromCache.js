// for loading event data into cache
const Pinball      = require('../lib/pinball');
const cache        = require('../lib/cache');
const Cache        = cache.Cache;
const sCacheUrl    = 'redis://localhost:6379';
const cacheDB      = new Cache({ sCacheUrl : sCacheUrl });

const cityCode     = 'nyc';

const t0 = Date.now();
let t1;

const scanPattern = `pb:${cityCode}:*`; // used for keys
const setKey      = cache.getSortedSetName(cityCode);
console.log('setKey',setKey);

cacheDB.orderedKeys({ setKey: setKey })
.then( aKeys => {
  t1 = Date.now();
  console.log('scanned keys length',aKeys.length,'time',t1-t0);
  return cacheDB.batchGetFromCache(aKeys);
})
.then( aObjects => {
  let t2 = Date.now();
  console.log('batchGetFromCache object length',aObjects.length,'time',t2-t1);
  process.exit(0);
})
.catch( err => {
  console.error({ action: 'loadCache.Promise.all.aUpsertPromises.err', err: err });
  throw err;
})


