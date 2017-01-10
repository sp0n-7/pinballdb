// for loading event data into cache
const cache        = require('../lib/cache');
const CacheWriter  = cache.CacheWriter;
const sCacheUrl    = 'redis://localhost:6379';
const cacheDB      = new CacheWriter({ sCacheUrl : sCacheUrl });


const getTime = (tClock) => {
  const dT = process.hrtime(tClock);
  return (dT[0]*1000) + (dT[1] / 1000000);
}

const NItems    = 100000;



let aIds   = [];
for (let i=0;i < NItems;i++) {
  const id = '-k' + i;
  aIds.push(id);
}

const t0 = Date.now();
cacheDB.batchRemoveFromCache(aIds).then( () => {
  console.log('clear cache time',Date.now()-t0);
})
.catch( err => {
  console.error({ action: 'clearCache.Promise.all.aRemovePromises.err', err: err });
  throw err;
})
