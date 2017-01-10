// used for kv doc storage
const url            = require('url');
const bluebird       = require('bluebird');
const redis          = require('redis');
bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(redis.Multi.prototype);

const getRedisClient = () => {
  const redisUrl  = url.parse('redis://localhost:6379');
  const client    = redis.createClient(redisUrl.port, redisUrl.hostname);
  // console.log({ action: 'app', redisUrl: redisUrl });
  if (redisUrl.auth) {
    client.auth(redisUrl.auth.split(":")[1]);
  }
  return client;
}

const pub = getRedisClient();
const sub = getRedisClient();

// let cursor = '0';

const scan = (pattern,cursorIn,returnSet) => {
  let cursor = cursorIn || '0';
  return pub.scanAsync(cursor, 'MATCH', pattern, 'COUNT', '100000').then( (res) => {
    cursor = res[0];
    if (Array.isArray(returnSet)) {
      returnSet = returnSet.concat(res[1]);
    }
    else {
      returnSet = res[1];
    }
    if(cursor === '0'){
      console.log('Scan Complete');
      return returnSet;
    }
    else {
      // do your processing
      return scan(pattern,cursor,returnSet);
    }
  });
}



const upsertDoc = (oDoc) => {
  return pub.hmsetAsync(oDoc.id, oDoc).then( (res) => {
    pub.publish('upsert', JSON.stringify(oDoc));
    return res;
  });
}

const removeDoc = (id) => {
  return getDoc(id).then( (oDoc) => {
    let aKeys = Object.keys(oDoc);
    return pub.hdelAsync(id,aKeys).then( (res) => {
      pub.publish('remove', id);
      return res;
    })
  })
}

const getDoc = (id) => {
  return pub.hgetallAsync(id).then( (res) => {
    pub.publish('get', id);
    return res;
  })
}

const getTime = (tClock) => {
  const dT = process.hrtime(tClock);
  return (dT[0]*1000) + (dT[1] / 1000000);
}

// psubscribe and pmessage is kinda nutz, emits disconnected keyspace and keyevent pmessages
// just manually publish and subscribe
// sub.on('psubscribe', function (channel, count) {
sub.on('subscribe', function (channel, count) {
  const a = { id: 'a', yoo: 'hoo', isGood: false, and: 100, isGreaterThan: 99.9 };

  const N = 1;

  const start = process.hrtime();

  let tUpSum = tGetSum = tDelSum = tAllSum = 0;

  let aPromises = [];

  for (let i = 0; i < N;i++) {
    let go = () => {
      let t1 = process.hrtime();
      let t2,t3;
      return new Promise( (resolve,reject) => {
        upsertDoc(a)
        .catch( err => {
          console.error('upsertDoc.err',err);
        })
        .then( () => {
          tUpSum += getTime(t1);
          t2 = process.hrtime();
          return getDoc(a.id);
        })
        .catch( err => {
          console.error('getDoc.err',err);
        })
        .then( () => {
          tGetSum += getTime(t2);
          t3 = process.hrtime();
          return removeDoc(a.id);
        })
        .catch( err => {
          console.error('removeDoc.err',err);
        })
        .then( () => {
          tDelSum += getTime(t3);
          return getDoc(a.id);
        })
        .catch( err => {
          console.error('getDocAfterRemoved.err',err);
        })
        .then( () => {
          tAllSum += getTime(t1);
          resolve();
        })      
      })
    }
    aPromises.push(go());
  }

  let t4;

  Promise.all(aPromises)
  .then( () => {
    t4 = process.hrtime();
    return scan('*');
  })
  .then( (aKeys) => {
    console.log('time',getTime(t4),'scan',aKeys);

    console.log({ action: 'test complete', tUpAvg: tUpSum/N, tGetAvg: tGetSum/N, tDelAvg: tDelSum/N, tAllAvg: tAllSum/N, tTotal: getTime(start) });
    process.exit(0);
  })
  .catch( err => {
    console.error({ action: 'test error', err:err });
    process.exit(1);    
  })

})

// sub.on('pmessage', (channel, message) => {
sub.on('message', (channel, message) => {
  let logMessage = message;
  if (channel === 'upsert') {
    logMessage = JSON.parse(message);
  }
  console.log('sub.on.message channel',channel,'message',logMessage);
});



// sub.psubscribe('*');
sub.subscribe('upsert');
sub.subscribe('remove');
sub.subscribe('get');








