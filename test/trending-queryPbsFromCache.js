const Pinballs     = require('../lib/pinballs');
const sCacheUrl    = 'redis://localhost:6379';

const getTime = (tClock) => {
  const dT = process.hrtime(tClock);
  return (dT[0]*1000) + (dT[1] / 1000000);
}

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
const NBucketThreshold = 5000;
const halfWinLonScale = 0.02;
const halfWinLatScale = 0.02;

const NItems    = 100000;
const NQueries  = 10000;
const cityCode  = 'nyc'
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

const tr = pbs.trs[cityCode]

tr.addSubscriber({ sCacheUrl: sCacheUrl })
.then( () => {

  const t0 = Date.now();

  const t1 = Date.now();
  console.log('load cache time',t1-t0);

  tr.printGrid();

  const t2 = Date.now();

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

    aResults.push(tr.query({
      lowerLatitude     : lowerLatitude,
      lowerLongitude    : lowerLongitude,
      upperLatitude     : upperLatitude,
      upperLongitude    : upperLongitude,
      N                 : N,
      minIncidentLevel  : 0
    }));

  }


  let t3 = Date.now();
  setTimeout( () => {
  console.log({ queriesTimeMS: t3-t2, queriesPerSecond: NQueries / ( (t3-t2)/1000 ) })
  // for (let ind=0;ind < aResults.length;ind++) {
  for (let ind=aResults.length - 10;ind < aResults.length;ind++) {
    // let ind = aResults.length - 1;
    console.log('iQuery',ind);
    for (let j=0;j < aResults[ind].length;j++) {
      console.log(aResults[ind][j].id,aResults[ind][j].ts,aResults[ind][j].latitude,aResults[ind][j].longitude)
    }    
  }

    console.log('delay done');
  },5000)


})
.catch( err => {
  console.error({ action: 'tr.addSubscriber.err', err: err, stack: err.stack });
  process.exit(1);
})

