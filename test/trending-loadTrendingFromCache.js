// for loading event data into cache
const Trending     = require('../index').Trending
const cache        = require('../lib/cache');
const Cache        = cache.Cache;
const sCacheUrl    = 'redis://localhost:6379';
const cacheDB      = new Cache({ sCacheUrl : sCacheUrl });

const cityCode     = 'nyc';

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
