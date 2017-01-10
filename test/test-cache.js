const cache           = require('../lib/cache');
const CacheSubscriber = cache.CacheSubscriber;
const CacheWriter     = cache.CacheWriter;


const getTime = (tClock) => {
  const dT = process.hrtime(tClock);
  return (dT[0]*1000) + (dT[1] / 1000000);
}

const sUrl = 'redis://localhost:6379';
const pub  = new CacheWriter({ sUrl: sUrl });

const oSubscriberOptions = {
  sUrl : sUrl,
  oProcs: {
    upsert: (oDoc) => {
      console.log({ action: 'subscriber.proc.upsert', oDoc:oDoc });
    },
    remove: (id) => {
      console.log({ action: 'subscriber.proc.remove', id: id });
    }
  }
}
new CacheSubscriber(oSubscriberOptions).then( sub => {

  const N = 100;

  const start = process.hrtime();

  let tUpSum = tGetSum = tDelSum = tAllSum = 0;

  let aPromises = [];

  for (let i = 0; i < N;i++) {
    const a = { id: 'a'+i, yoo: 'hoo', isGood: false, and: 100, isGreaterThan: 99.9 };
  
    let go = () => {
      let t1 = process.hrtime();
      let t2,t3;
      return new Promise( (resolve,reject) => {
        pub.upsert(a)
        .catch( err => {
          console.error('upsert.err',err);
        })
        .then( () => {
          tUpSum += getTime(t1);
          t2 = process.hrtime();
          return pub.get(a.id);
        })
        .catch( err => {
          console.error('get.err',err);
        })
        .then( () => {
          tGetSum += getTime(t2);
          t3 = process.hrtime();
          return pub.remove(a.id);
        })
        .catch( err => {
          console.error('remove.err',err);
        })
        .then( () => {
          tDelSum += getTime(t3);
          return pub.get(a.id);
        })
        .catch( err => {
          console.error('getAfterRemoved.err',err);
        })
        .then( () => {
          tAllSum += getTime(t1);
          resolve();
        })      
      })
    }
    aPromises.push(go());
  }

  const delayAfterDone = 500;

  Promise.all(aPromises)
  .then( () => {
    // gotta wait for remove promise
    setTimeout( () => {
      console.log({ action: 'test complete', tUpAvg: tUpSum/N, tGetAvg: tGetSum/N, tDelAvg: tDelSum/N, tAllAvg: tAllSum/N, tTotal: getTime(start) - delayAfterDone });
      process.exit(0);
    }, delayAfterDone);
  })
  .catch( err => {
    console.error({ action: 'test error', err:err });
    process.exit(1);
  })

})









