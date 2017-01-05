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

const client = getRedisClient();

const upsertDoc = (oDoc) => {
  return client.hmsetAsync(oDoc.id, oDoc);
}

const removeDoc = (id) => {
  return getDoc(id).then( (oDoc) => {
    let aKeys = Object.keys(oDoc);
    return client.hdelAsync(id,aKeys);
  })
}

const getDoc = (id) => {
  return client.hgetallAsync(id);
}

const getTime = (tClock) => {
  const dT = process.hrtime(tClock);
  return (dT[0]*1000) + (dT[1] / 1000000);
}

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

Promise.all(aPromises)
.then( () => {
  console.log({ action: 'test complete', tUpSum: tUpSum, tGetSum: tGetSum, tDelSum: tDelSum, tAllSum: tAllSum, tTotal: getTime(start) });
})
.catch( err => {
  console.error({ action: 'test error', err:err });
})





