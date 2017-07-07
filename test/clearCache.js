// for loading event data into cache
const cache        = require('../lib/cache');
const Cache        = cache.Cache;
const getCacheId   = cache.getCacheId;
const sCacheUrl    = 'redis://localhost:6379';

const cacheDB      = new Cache({ sCacheUrl : sCacheUrl });

const cityCode     = 'nyc';
// const cityCode     = 'la';

const getTime = (tClock) => {
  const dT = process.hrtime(tClock);
  return (dT[0]*1000) + (dT[1] / 1000000);
}

const NItems    = 100000;



let aCacheIds   = [];
for (let i=0;i < NItems;i++) {
  const id = '-k' + i;
  aCacheIds.push(getCacheId({ id: id }));
}

const t0 = Date.now();
cacheDB.keys({ pattern: `pb:*`})
.then( aKeys => {
  console.log('keys before',aKeys);
  return cacheDB.batchRemoveFromCache({ aCacheIdArray: aCacheIds, cityCode: cityCode })
})  
.then( () => {
  console.log('clear cache time',Date.now()-t0);
  return cacheDB.keys({ pattern: '*'});
})
.then( aKeys => {
  console.log('keys after',aKeys);
//   return cacheDB.batchRemoveFromCache({ aCacheIdArray: aKeys, cityCode: cityCode });
// })
// .then( () => {
//   return cacheDB.keys({ pattern: '*'});
// })
// .then( aKeys => {
//   console.log('final keys after',aKeys);
  process.exit(0);
})
.catch( err => {
  console.error({ action: 'clearCache.Promise.all.aRemovePromises.err', err: err });
  throw err;
})
