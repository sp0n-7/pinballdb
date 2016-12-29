const pg      = require('pg');
const Pool    = require('pg-pool');

const config = {
  // user: 'foo', //env var: PGUSER
  database: 'messel', //env var: PGDATABASE
  // password: 'secret', //env var: PGPASSWORD
  host: 'localhost', // Server hosting the postgres database
  port: 5432, //env var: PGPORT
  max: 20, // max number of clients in the pool
  idleTimeoutMillis: 30000, // how long a client is allowed to remain idle before being closed
};

//this initializes a connection pool
//it will keep idle connections open for a 30 seconds
//and set a limit of maximum 10 idle clients


const pool = new Pool();

// both native versions require promisfying
// const nativeClient  = require('pg').native.Client
// const pool          = new Pool({ Client: NativeClient })

// const PgNativeClient  = require('pg-native')
// const pool            = new Pool({ Client: PgNativeClient })

pool.on('error', function (err, client) {
  // if an error is encountered by a client while it sits idle in the pool
  // the pool itself will emit an error event with both the error and
  // the client which emitted the original error
  // this is a rare occurrence but can happen if there is a network partition
  // between your application and the database, the database restarts, etc.
  // and so you might want to handle it and at least log it out
  console.error('idle client error', err.message, err.stack)
})

const sTable = 'incidents';


const center    = [-73.993549, 40.727248];
const lowerLeft = [-74.009180, 40.716425];
const deltaLon  = 2 * Math.abs(lowerLeft[0] - (-73.97725));
const deltaLat  = 2 * Math.abs(lowerLeft[1] - (40.7518692));

const NQueries = 10000;
const N = 20;


let t0 = Date.now();

let aPromises = [];
for (let i=0;i < NQueries;i++) {
  const searchLon       = lowerLeft[0] + Math.random() * deltaLon;
  const searchLat       = lowerLeft[1] + Math.random() * deltaLat;
  const halfWinLon      = Math.random() * 0.04;
  const halfWinLat      = Math.random() * 0.04;

  const lowerLatitude   = searchLat - halfWinLat;
  const lowerLongitude  = searchLon - halfWinLon;
  const upperLatitude   = searchLat + halfWinLat;
  const upperLongitude  = searchLon + halfWinLon;

  const fQuery = () => {
    const sAction = 'fQuery';
    return pool.connect().then( (client) => {
      const aCommands = [ 
        `select ts, data::JSON from ${sTable}`,      
        'where (latitude >= $1) and (latitude <= $2) and (longitude >= $3) and (longitude <= $4)',
        'order by ts desc',
        `limit ${N} offset 0;`
      ];
      const aArgs    = [lowerLatitude,upperLatitude,lowerLongitude,upperLongitude];
      // console.log({ action: sAction + '.bounds', aArgs: aArgs });

      return client.query(aCommands.join('\n'), aArgs).then( (result) => {
        client.release();
        // console.log({ action: sAction + '.query.success', result:JSON.stringify(result,null,2) });
        return result;
      })
      .catch( (err) => {
        if (typeof client.release === 'function') client.release();
        else {
          console.info({ action: sAction + '.query.client.has.no.release', client: client });
        }
        return Promise.reject(err);
      })
    })
  }

  let aQuery = fQuery();

  aPromises.push(aQuery);
}


Promise.all(aPromises).then( (aResults) => {
  let t1 = Date.now();
  console.log({ queriesTimeMS: t1-t0, queriesPerSecond: NQueries / ( (t1-t0)/1000 ) })

  // for (let i in aResults) {
  //   const result = aResults[i].rows;
  //   const aKeys = result.map( (oMatch) => {
  //     return { id: oMatch.data.id, ts: oMatch.data.ts, latitude: oMatch.data.latitude, longitude: oMatch.data.longitude } 
  //   })
  //   console.log({ action: 'query', aKeys: aKeys });
  // }
  process.exit(0);
})
.catch( (err) => {
  throw err;
})       
