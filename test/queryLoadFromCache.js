const Pinball      = require('../lib/pinball');

// for loading event data into cache
const cache        = require('../lib/cache');
const Cache        = cache.Cache;
const sCacheUrl    = 'redis://localhost:6379';
const cacheDB      = new Cache({ sCacheUrl : sCacheUrl });

const cityCode     = 'nyc';


const getTime = (tClock) => {
  const dT = process.hrtime(tClock);
  return (dT[0]*1000) + (dT[1] / 1000000);
}


const center    = [-73.993549, 40.727248];
const lowerLeft = [-74.009180, 40.716425];
const deltaLon  = 2 * Math.abs(center[0] - lowerLeft[0]);
const deltaLat  = 2 * Math.abs(center[1] - lowerLeft[1]);

// works in conjuction with NBucketThreshold the algorithm switch
//   if N total within buckets > threshold does full scan backwards on ordered array of events
//   else it takes all bucket arrays, combines, sorts and keeps N highest (faster than select N tree methods explored)
// if the most likely query is large, smaller bucket dims work faster, due to quicker intermediate grid sums
const NLat = 40;
const NLon = 40;
const NBucketThreshold = 5000;
const halfWinLonScale = 0.001;
const halfWinLatScale = 0.001;

const NQueries  = 100000;

const pb = new Pinball({
  lowerLatitude     : lowerLeft[1],
  upperLatitude     : lowerLeft[1] + deltaLat,
  lowerLongitude    : lowerLeft[0],
  upperLongitude    : lowerLeft[0] + deltaLon,
  NLatitude         : NLat,
  NLongitude        : NLon,
  NBucketThreshold  : NBucketThreshold
});


const t0 = Date.now();
pb.addSubscriber({ sCacheUrl: sCacheUrl, cityCode: cityCode })
.then( () => {
  const t1 = Date.now();
  console.log('addSubscriber and load from cache',t1-t0);

  pb.printGrid();

  const N = 20;

  let aResults = [];
  // let aPromises = [];
  for (let i=0;i < NQueries;i++) {
    const searchLon       = lowerLeft[0] + Math.random() * deltaLon;
    const searchLat       = lowerLeft[1] + Math.random() * deltaLat;
    const halfWinLon      = Math.random() * halfWinLonScale;
    const halfWinLat      = Math.random() * halfWinLatScale;

    const lowerLatitude   = searchLat - halfWinLat;
    const lowerLongitude  = searchLon - halfWinLon;
    const upperLatitude   = searchLat + halfWinLat;
    const upperLongitude  = searchLon + halfWinLon;

    // console.log('search args', lowerLatitude,lowerLongitude,upperLatitude,upperLongitude,N)

    aResults.push(pb.query({
      lowerLatitude   : lowerLatitude,
      lowerLongitude  : lowerLongitude,
      upperLatitude   : upperLatitude,
      upperLongitude  : upperLongitude,
      N               : N
    }));

  }


  let t2 = Date.now();
  console.log({ queriesTimeMS: t2-t1, queriesPerSecond: NQueries / ( (t2-t1)/1000 ) })
  // for (let ind=0;ind < aResults.length;ind++) {
  let ind = aResults.length - 1;
  console.log('iQuery',ind);
  for (let j=0;j < aResults[ind].length;j++) {
    console.log(aResults[ind][j].id,aResults[ind][j].ts,aResults[ind][j].latitude,aResults[ind][j].longitude)
  } 
  process.exit(0);
  // }

})
.catch( err => {
  console.error({ action: 'pb.addSubscriber.err', err: err, stack: err.stack });
  process.exit(1);
})

