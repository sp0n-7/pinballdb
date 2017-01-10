const Pinball      = require('../lib/pinball');


const getTime = (tClock) => {
  const dT = process.hrtime(tClock);
  return (dT[0]*1000) + (dT[1] / 1000000);
}

const sCacheUrl = 'redis://localhost:6379';

const center    = [-73.993549, 40.727248];
const lowerLeft = [-74.009180, 40.716425];
const deltaLon  = 2 * Math.abs(center[0] - lowerLeft[0]);
const deltaLat  = 2 * Math.abs(center[1] - lowerLeft[1]);

// explored 40x40,20x20,10x10
// works in conjuction with NBucketThreshold the algorithm switch
//   if N total within buckets > threshold does full scan backwards on ordered array of events
//   else it takes all bucket arrays, combines, sorts and keeps N highest (faster than select N tree methods explored)
// if the most likely query is large, smaller bucket dims work faster, due to quicker intermediate grid sums
const NLat = 14;
const NLon = 14;
const NBucketThreshold = 5000;
const halfWinLonScale = 0.001;
const halfWinLatScale = 0.001;

const NItems    = 100000;
const NQueries  = 1;

const pb = new Pinball({
  lowerLatitude     : lowerLeft[1],
  upperLatitude     : lowerLeft[1] + deltaLat,
  lowerLongitude    : lowerLeft[0],
  upperLongitude    : lowerLeft[0] + deltaLon,
  NLatitude         : NLat,
  NLongitude        : NLon,
  NBucketThreshold  : NBucketThreshold,
  cacheUrl          : sCacheUrl
});

pb.addSubscriber(sCacheUrl)
.then( () => {
  const oIncidentBase = {
    "address" : "780 3rd Ave, New York, NY 10017, USA",
    "cityCode" : "nyc",
    "hasVod" : true,
    "level" : 1,
    "liveStreamers" : {
      "de8bb371f14d4bdc80d5" : {
        "cs" : 1477496059721,
        "hlsDone" : true,
        "ll" : [ 40.75488417878817, -73.9718003757987 ],
        "ts" : 1477496917025,
        "username" : "JR",
        "videoStreamId" : "747392c4-15ae-492b-b732-eca3b4015e8b"
      }
    },
    "location" : "780 3rd Avenue",
    "neighborhood" : "Midtown East",
    "placeName" : "780 3rd Ave",
    "raw" : "Large Group of Protesters at 780 3rd Ave, New York, NY 10017, USA",
    "rawLocation" : "780 3rd Ave, New York, NY 10017, USA",
    "sessionId" : "lucho",
    "status" : "active",
    "title" : "Large Group of Protesters Outside Senator's Office, Arrests Made",
    "transcriber" : "google:104637345869051085501",
    "updates" : {
      "-KV0Op02CRONAiUiE43F" : {
        "text" : "Incident reported at 780 3rd Avenue.",
        "ts" : 1477492297832
      },
      "-KV0OzM4fRVWfRX5WEY0" : {
        "text" : "Police are on scene where approximately 100 protesters are gathered.",
        "ts" : 1477492340089
      },
      "-KV0P-dArBk_ml-WN-L1" : {
        "text" : "The 17th Precinct sergeant has requested a Level-One Mobilization for crowd control.",
        "ts" : 1477492345360
      },
      "-KV0PYmTJzrvpjZgocvr" : {
        "text" : "The NYPD's Strategic Response Group (SRG) is sending officers to the scene.",
        "ts" : 1477492489274
      },
      "-KV0Q31Z-i3GAMqQprOX" : {
        "text" : "The NYPD Legal Bureau has been requested, along with the Technical Assistance Response Unit (TARU).",
        "ts" : 1477492621404
      },
      "-KV0RPOTNcAoJfdAiFqK" : {
        "text" : "The protest may be related to the legality of Airbnb operations in New York City.",
        "ts" : 1477492975121
      },
      "-KV0SfVL_ncfV6FForlA" : {
        "text" : "Police are bringing the Long Range Acoustic Device (LRAD) public address system.",
        "ts" : 1477493307323
      },
      "-KV0UndpIVyL9YZzIaRz" : {
        "hlsReady" : true,
        "text" : "JR is live on the scene.",
        "ts" : 1477493865124,
        "uid" : "de8bb371f14d4bdc80d5",
        "videoStreamId" : "11709af9-acb4-42cd-bddb-735c1e4fe5ca"
      },
      "-KV0VVUiWIDGirlQwG5_" : {
        "text" : "US Senator Charles Schumer has an office in the building.",
        "ts" : 1477494046441
      },
      "-KV0VX_RglFVIYMUYFoP" : {
        "text" : "The protest is over construction of a pipeline.",
        "ts" : 1477494057215
      },
      "-KV0Vz0gSMUsK7fgf7EP" : {
        "text" : "People are drumming and chanting on the sidewalk.",
        "ts" : 1477494173716
      },
      "-KV0YRCCRimB1WLIM27G" : {
        "text" : "Police are bringing wagons to transport people who may be arrested.",
        "ts" : 1477494817503
      },
      "-KV0Y_iJkXVJI1DfTjQX" : {
        "text" : "Some of the protesters are behind a railing; others may be blocking a building's entrance.",
        "ts" : 1477494856471
      },
      "-KV0an6NbOsp4ZC4f5X7" : {
        "text" : "Police have used a loudspeaker to ask those blocking the doorway to stand up and disperse.",
        "ts" : 1477495697799
      },
      "-KV0bXm-ukjkBux0ED7x" : {
        "text" : "People are now being arrested by NYPD officers.",
        "ts" : 1477495892926
      },
      "-KV0bbpyzVp6tgeZ05-j" : {
        "text" : "Police are using plastic zip ties to restrain the hands of some of those who were sitting in front of the building's entrance.",
        "ts" : 1477495913754
      },
      "-KV0byJG0nhwIlIvexOs" : {
        "text" : "Approximately 15 people have been arrested so far, police report.",
        "ts" : 1477496005798
      },
      "-KV0c9hOh0u3LszWgSct" : {
        "hlsDone" : true,
        "hlsVodDone" : true,
        "text" : "JR has stopped broadcasting.",
        "ts" : 1477496056717,
        "uid" : "de8bb371f14d4bdc80d5",
        "videoStreamId" : "11709af9-acb4-42cd-bddb-735c1e4fe5ca"
      },
      "-KV0cAT-HJGVew3Ncfuu" : {
        "hlsReady" : true,
        "text" : "JR is live on the scene.",
        "ts" : 1477496059828,
        "uid" : "de8bb371f14d4bdc80d5",
        "videoStreamId" : "747392c4-15ae-492b-b732-eca3b4015e8b"
      },
      "-KV0eOUFnI3O7H4bqk1R" : {
        "hlsDone" : true,
        "hlsVodDone" : true,
        "text" : "JR has stopped broadcasting.",
        "ts" : 1477496641540,
        "uid" : "de8bb371f14d4bdc80d5",
        "videoStreamId" : "747392c4-15ae-492b-b732-eca3b4015e8b"
      }
    },
    "whoa" : false
  }

  const t0 = Date.now();

  let aItems = [];
  let aIds   = [];
  // let aUpsertPromises = [];
  for (let i=0;i < NItems;i++) {
    const id = '-k' + i;
    const ll = [lowerLeft[1] + Math.random() * deltaLat,lowerLeft[0] + Math.random() * deltaLon];
    const oItemBase = Object.assign({}, oIncidentBase);
    const oItem = Object.assign(oItemBase, {
      id          : id,
      cs          : t0 - 10 * 60 * 1000 + i,
      ts          : t0 + i,
      score       : t0 + i,
      latitude    : ll[0],
      longitude   : ll[1],
      ll          : ll,
      key         : id
    });

    // sync direct local upsert
    // pb._upsert(oItem)

    // async cache, so all pinballs are updated
    // redis protocol issues with 100k+ concurrent
    // aUpsertPromises.push(pb.upsertCache(oItem));
    aItems.push(oItem);
    aIds.push(id);
  }

  // Promise.all(aUpsertPromises).then( () => {
  pb.batchUpsertCache(aItems).then( () => {
    const t1 = Date.now();
    console.log('load cache time',t1-t0);

    // now all events have been added to the cache
    // but pubsub flow and local upsert latency occurs

    setTimeout( () => {
      pb.printGrid();

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

        aResults.push(pb.query({
          lowerLatitude   : lowerLatitude,
          lowerLongitude  : lowerLongitude,
          upperLatitude   : upperLatitude,
          upperLongitude  : upperLongitude,
          N               : N
        }));

      }


      let t3 = Date.now();
      console.log({ queriesTimeMS: t3-t2, queriesPerSecond: NQueries / ( (t3-t2)/1000 ) })
      // for (let ind=0;ind < aResults.length;ind++) {
      let ind = aResults.length - 1;
      console.log('iQuery',ind);
      for (let j=0;j < aResults[ind].length;j++) {
        console.log(aResults[ind][j].id,aResults[ind][j].ts,aResults[ind][j].latitude,aResults[ind][j].longitude)
      }    
      // }

      // lets clear the cache and check that pubsub flow
      // let aRemovePromises = [];
      // for (let i=0;i < NItems;i++) {
      //   const id = '-k' + i;
      //   aRemovePromises.push(pb.removeFromCache(id));
      // }
      let t4 = Date.now();
      // Promise.all(aRemovePromises).then( () => {
      pb.batchRemoveFromCache(aIds).then( () => {
        console.log('clear cache time',Date.now()-t4);

        // now all events have been removed from cache
        // but pubsub flow and local upsert may not have completed
        setTimeout( () => {
          pb.printGrid();
          process.exit(0);

        },500);

      })
      .catch( err => {
        console.error({ action: 'query.Promise.all.aRemovePromises.err', err: err });
        throw err;
      })

    },500)
  })
  .catch( err => {
    console.error({ action: 'query.Promise.all.aUpsertPromises.err', err: err });
    throw err;
  })
})
.catch( err => {
  console.error({ action: 'pb.addSubscriber.err', err: err });
  throw err;
})

