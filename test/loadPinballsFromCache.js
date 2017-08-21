// for loading event data into cache
const cache        = require('../lib/cache');
const Cache        = cache.Cache;
const sCacheUrl    = 'redis://localhost:6379';
const cacheDB      = new Cache({ sCacheUrl : sCacheUrl,setName:'pb',scoreProperty: 'cs' });

const Pinballs     = require('../lib/pinballs');

const options = {
  nyc: {
    lowerLatitude     : 40.47724766391948,
    upperLatitude     : 40.93037458898227,
    lowerLongitude    : -74.26277160644531,
    upperLongitude    : -73.71345520019531,
    NLatitude         : 40,
    NLongitude        : 40,
    NBucketThreshold  : 5000   
  },
  la: {
    lowerLatitude     : 33.40163829558248,
    upperLatitude     : 34.3366324743773,
    lowerLongitude    : -118.7017822265625,
    upperLongitude    : -117.13897705078125,
    NLatitude         : 100,
    NLongitude        : 100,
    NBucketThreshold  : 5000   
  }
};


const pbs = new Pinballs(options);
console.log('pbs',pbs);

const t1 = Date.now();

const oSubOptions = {
  nyc: {
    sCacheUrl: sCacheUrl
  },
  la: {
    sCacheUrl: sCacheUrl
  }
}


pbs.addSubscriber(oSubOptions)
.then( () => {
  console.log({ action: 'pbs.loadFromCache.complete', time: Date.now() - t1})
  process.exit(0);
})
.catch( err => {
  console.error({ action: 'pbs.loadFromCache.err', err:err });
  process.exit(1);
})
