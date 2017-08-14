// for loading event data into cache
const Trending     = require('../index').Trending
const cache        = require('../lib/cache');
const Cache        = cache.Cache;
const cacheBase    = cache.CacheBase
const sCacheUrl    = 'redis://localhost:6379';
const cacheDB      = new Cache({ sCacheUrl : sCacheUrl });

const cityCode     = 'nyc';

// ny like grid
const lowerLeft  = [-74.262771, 40.477247];
const upperRight = [-73.713455, 40.930374];
const deltaLon   = upperRight[0] - lowerLeft[0];
const deltaLat   = upperRight[1] - lowerLeft[1];


// works in conjuction with NBucketThreshold the algorithm switch
//   if N total within buckets > threshold does full scan backwards on ordered array of events
//   else it takes all bucket arrays, combines, sorts and keeps N highest (faster than select N tree methods explored)
// if the most likely query is large, smaller bucket dims work faster, due to quicker intermediate grid sums
const NLat = 40;
const NLon = 40;
const halfWinLonScale = 0.001;
const halfWinLatScale = 0.001;

const tr = new Trending({
  cityCode          : cityCode,
  lowerLatitude     : lowerLeft[1],
  upperLatitude     : lowerLeft[1] + deltaLat,
  lowerLongitude    : lowerLeft[0],
  upperLongitude    : lowerLeft[0] + deltaLon,
  NLatitude         : NLat,
  NLongitude        : NLon,
});

const t1 = Date.now();
tr.loadFromCache({ sCacheUrl: sCacheUrl, setName: 'tr' })
.then( () => {
  console.log({ action: 'tr.loadFromCache.complete', time: Date.now() - t1})
  process.exit(0);
})
.catch( err => {
  console.error({ action: 'tr.loadFromCache.err', err:err });
  process.exit(1);
})


