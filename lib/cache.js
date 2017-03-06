const url               = require('url');
const bluebird          = require('bluebird');
const redis             = require('redis');
bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(redis.Multi.prototype);

const Promise           = bluebird;


// const sRedisUrl = 'redis://localhost:6379';

// helper functions
const getRedisClient = (sUrl) => {
  const redisUrl  = url.parse(sUrl);
  const client    = redis.createClient(redisUrl.port, redisUrl.hostname, {no_ready_check: true});
  // console.log({ action: 'app', redisUrl: redisUrl });
  if (redisUrl.auth) {
    client.auth(redisUrl.auth.split(":")[1]);
  }
  return client;
}

const getCacheId = (options) => {
  return `pb:${options.cityCode}:${options.id}`;
}

const getDocId = (cacheId) => {
  const sAction = 'getDocId';
  let   oDocId  = null;
  if (typeof cacheId === 'string') {
    let aResults = cacheId.match(/pb:(.*):(.*)/);
    if (Array.isArray(aResults) && aResults.length >= 3) {
      oDocId = { cityCode: aResults[1], id: aResults[2] }
    }
    else {
      console.error({ action: sAction + '.err', cacheId: cacheId })
    }
  }
  return oDocId;
}

// currently same format as getCacheId 
const getCacheChannel = (options) => {
  return `pb:${options.cityCode}:${options.action}`;
}

const getSortedSetName = (cityCode) => {
  return `pb:${cityCode}:sortedSet`;
}

const oChannels = {
  upsert: 'upsert',
  remove: 'remove'
}

// note:
// redis psubscribe and pmessage is kinda nutz
// it emits disconnected keyspace and keyevent pmessages
// manually publish and subscribe instead



class CacheBase {
  constructor(options) {
    // this.sClass = 'Cache';
    this.client = getRedisClient(options.sCacheUrl);
  }
}

// add city subscribe actions
// Once the client enters the subscribed state 
// it is not supposed to issue any other commands
// except further subscribe commands, https://redis.io/commands/subscribe
class CacheSubscriber extends CacheBase {
  
  // options { sUrl: sRedisUrl, oProcs:{ pb:${cityCode}:upsert: fUpsert, pb:${cityCode}:remove: fRemove }}
  // returns a Promise which resolves after subscriptions are set
  // new CacheSubscriber(options).then( (instance) => { ... })
  constructor(options) { 
    const sAction = CacheSubscriber.name + '.constructor';

    super(options);
    this.oProcs     = options.oProcs;
    const aChannels = Object.keys(this.oProcs);
    // console.log('aChannels',aChannels);

    let aSubscribePromises = [];
    for (let i in aChannels) {
      const sChannel = aChannels[i];
      aSubscribePromises.push(this.client.subscribeAsync(sChannel));
    }

    this.client.on('message', (channel, message) => {

      let messageToSend = message;
      if (channel.includes(oChannels.upsert)) {
        try { 
          messageToSend = JSON.parse(message);
        }
        catch (err) {
          console.error({action: sAction + '.parse.err', message: message, err: err });
        }
      }
      // console.log({ action: sAction + '.on.message', channel: channel, message: messageToSend });
      
      // handle fProcs
      let fProc = this.oProcs[channel];
      if (typeof fProc === 'function') {
        fProc(messageToSend)
      }
    });

    // return a promise
    return Promise.all(aSubscribePromises).then( () => {
      return this;
    })  
  }
}

const subscribeToCache = (options) => {
  return new CacheSubscriber(options);
}

// add city to write/publish actions
// reader, writer, and publisher
class Cache extends CacheBase {
  constructor(options) {
    super(options);
  }

  getWithCacheId(cacheId) {
    return this.client.getAsync(cacheId).then( sData => {
      return JSON.parse(sData);
    })
  }

  multiGetWithCacheIds(aKeys) {
    if (Array.isArray(aKeys) && aKeys.length > 0) {
      return this.client.mgetAsync(aKeys).then( aData => {
        for (let i in aData) {
          aData[i] = JSON.parse(aData[i]);
        }
        return aData;
      });
    }
    else {
      return Promise.resolve([]);
    }
  }

  getWithDocId(options) {
    const cacheId = getCacheId(options);
    return this.client.getAsync(cacheId).then( sData => {
      return JSON.parse(sData);
    })
  }

  keys(options) {
    const sAction   = Cache.name + '.keys';
    const pattern   = options.pattern;
    let   cursorIn  = options.cursor;
    let   returnSet = options.returnSet;

    if (typeof pattern !== 'string' || pattern.length <= 0) {
      return Promise.reject(Error(`${sAction}.input.err options ${JSON.stringify(options)}`))
    }


    const batchSize = '' + 100000; // string input

    let cursor = cursorIn || '0';
    return this.client.scanAsync(cursor, 'MATCH', pattern, 'COUNT', batchSize).then( (res) => {
      cursor = res[0];
      if (Array.isArray(returnSet)) {
        returnSet = returnSet.concat(res[1]);
      }
      else {
        returnSet = res[1];
      }
      if(cursor === '0'){
        // console.log({ action : sAction + '.scan.complete', size: returnSet.length });
        return returnSet;
      }
      else {
        // do your processing
        return this.keys({ pattern: pattern, cursor: cursor, returnSet: returnSet });
      }
    });
  }

  orderedKeys(options) {
    const sAction   = Cache.name + '.keys';
    const setKey    = options.setKey;

    if (typeof setKey !== 'string' || setKey.length <= 0) {
      return Promise.reject(Error(`${sAction}.input.err options ${JSON.stringify(options)}`))
    }

    return this.client.zrangeAsync(setKey, 0, -1);
  }

  batchImport(options) {
    // zrangeAsync
    const sAction   = Cache.name + '.keys';
    const setKey    = options.setKey;
    const fProc     = options.fProc;  // sync function
    const NBatch    = options.NBatch || 1000;
    let   iStart    = options.iStart || 0;
    let   iEnd      = options.iEnd   || NBatch - 1;
    let   NTotal    = options.NTotal || 0;

    if (typeof setKey !== 'string' || setKey.length <= 0 || typeof fProc !== 'function') {
      return Promise.reject(Error(`${sAction}.input.err options ${JSON.stringify(options)}`))
    }

    const t0 = Date.now();
    let t1,t2;

    const processNextGroup = () => {                 
      return this.client.zrangeAsync(setKey, iStart, iEnd) // inclusive
      .then( aKeys => {
        t1 = Date.now();
        console.log('batchImport iStart',iStart,'iEnd',iEnd,'time',t1-t0);
        // get from cache
        // exec a supplied func, local upsert
        return this.processFromCache({ aKeys: aKeys, fProc: fProc })
        .then( () => {
          t2 = Date.now();
          console.log('batchProcessFromCache iStart',iStart,'iEnd',iEnd,'time',t2-t1);
          return aKeys.length;
        })
      })
    }

    return processNextGroup().then( NKeys => {
      let NKeysRequested = iEnd - iStart + 1;
      NTotal += NKeys;

      // full batch keep going
      if (NKeys === NKeysRequested) {
        iStart = iEnd + 1;
        iEnd   = iStart + NBatch - 1;
        return this.batchImport({ setKey: setKey, fProc: fProc, NBatch: NBatch, iStart: iStart, iEnd: iEnd, NTotal: NTotal });
      }
      else {
        // else we're done return NTotal processed
        return NTotal;
      }
    })
  }

  upsert(oDoc) {
    const cityCode      = oDoc.cityCode;
    const id            = oDoc.id;
    const action        = 'upsert';
    const cacheId       = getCacheId({ id: id, cityCode: cityCode});
    const cacheChannel  = getCacheChannel({ action: action, cityCode: cityCode });
    const sortedSet     = getSortedSetName(cityCode);

    let aPromises = [];
    aPromises.push(this.client.setAsync(cacheId, JSON.stringify(oDoc)));
    aPromises.push(this.client.zaddAsync(sortedSet, oDoc.cs, cacheId));

    return Promise.all(aPromises).then( aRes => {
      return this.client.publishAsync(cacheChannel, JSON.stringify(oDoc)).then( () => {
        return aRes[0];
      })      
    })
    
    // return this.client.setAsync(cacheId, JSON.stringify(oDoc)).then( (res) => {
    //   return this.client.publishAsync(cacheChannel, JSON.stringify(oDoc)).then( () => {
    //     return res;
    //   })
    // });
  }

  upsertFromIncident(options) {
    const id                  = options.id;
    const oIncident           = options.val;
    const oCacheIncident      = Object.assign({},oIncident);
    oCacheIncident.id         = id;
    oCacheIncident.score      = oCacheIncident.ts || oCacheIncident.cs;
    if (typeof oIncident === 'object' && 
        Array.isArray(oIncident.ll)   && oIncident.ll.length >= 2) 
    {
        oCacheIncident.latitude  = oIncident.ll[0];
        oCacheIncident.longitude = oIncident.ll[1];
      }
    }
    return this.upsert(oCacheIncident);
  } 
  
  // cacheId: 'pb:$cityCode:$docId' }
  removeCacheId(cacheId) {
    const oDocId        = getDocId(cacheId);
    const action        = 'remove';
    const cacheChannel  = getCacheChannel({ cityCode: oDocId.cityCode, action: action });
    const sortedSet     = getSortedSetName(oDocId.cityCode);

    let aPromises = [];
    aPromises.push(this.client.delAsync(cacheId));
    aPromises.push(this.client.zremAsync(sortedSet, cacheId));

    return Promise.all(aPromises).then( aRes => {
      return this.client.publishAsync(cacheChannel, cacheId).then( () => {
        return aRes[0];
      });
    });

    // return this.client.delAsync(cacheId).then( (res) => {
    //   return this.client.publishAsync(cacheChannel, cacheId).then( () => {
    //     return res;
    //   });
    // });
  }

  // { id: $docId, cityCode: $cityCode }
  removeDoc(options) {
    const oDocId   = options.id;
    const cityCode = options.cityCode;
    const action   = 'remove';
    const cacheId  = getCacheId({ id: oDocId, cityCode: cityCode });
    
    return this.removeCacheId(cacheId);
  }

  batchGetFromCache(options) {
    const sAction = Cache.name + '.batchGetFromCache';
    const NBatch  = 5000;

    const aCacheIdArray = options.aCacheIds;
    const aProps        = options.aProps; // pluck these properties if provided
    let   iStart        = options.iStart;
    let   aDocs         = options.aDocs;

    iStart = iStart || 0;
    aDocs  = Array.isArray(aDocs) ? aDocs : [];

    const getNextBatch = (aArray,iStart,NBatchSize,aFullDocs) => {
      return new Promise( (resolve,reject) => {
        let batch = this.client.batch();
        let iEnd  = iStart + NBatchSize;
        for (let i = iStart;i < iEnd && i < aArray.length;i++) {
          const cacheId = aArray[i];
          batch.get(cacheId);
        }
        batch.exec( (err,aResponse) => {
          let aBatchDocs = [];
          if (Array.isArray(aResponse)) {
            for (let i = 0; i < aResponse.length;i++) {
              const sDocString = aResponse[i];
              // skip and log malformed docstrings
              try {
                let oDoc = JSON.parse(sDocString);
                let oPlucked = {};
                if (Array.isArray(aProps)) {
                  for (let j = 0;j < aProps.length;j++) {
                    const sProp = aProps[j];
                    oPlucked[sProp] = oDoc[sProp];
                  }
                }
                else {
                  oPlucked = oDoc;
                }
                aBatchDocs.push(oPlucked);
              }
              catch (err) {
                console.error({ action: sAction + '.parse.err', sDocString: sDocString });
              }
            }
          }
          aResponse = null;
          resolve(aFullDocs.concat(aBatchDocs));
        });
      });
    }

    return getNextBatch(aCacheIdArray,iStart,NBatch,aDocs)
    .then( aFullDocs => {
      // console.log('istart',iStart,'aFullDocs.length',aFullDocs.length);
      if (aFullDocs.length < aCacheIdArray.length) {
        iStart = iStart + NBatch;
        return this.batchGetFromCache({ aCacheIds: aCacheIdArray, aProps: aProps, iStart: iStart, aDocs: aFullDocs });
      }
      else {
        return aFullDocs;
      }
    })
  }

  processFromCache(options) {
    const sAction       = Cache.name + '.batchProcessFromCache';
    const aKeys         = options.aKeys;
    const fProc         = options.fProc; // sync function

    return new Promise( (resolve,reject) => {
      let batch = this.client.batch();
      for (let i = 0;i < aKeys.length;i++) {
        const cacheId = aKeys[i];
        batch.get(cacheId);
      }
      batch.exec( (err,aResponse) => {
        if (err) {
          reject(err);
        } 
        else {
          let aDocs = [];
          if (Array.isArray(aResponse)) {
            for (let i = 0; i < aResponse.length;i++) {
              const sDocString = aResponse[i];
              // skip and log malformed docstrings
              try {
                let oDoc = JSON.parse(sDocString);
                fProc(oDoc);  
              }
              catch (err) {
                console.error({ action: sAction + '.parse.err', sDocString: sDocString });
              }
            }
          }
          resolve();
        }
      })
    });      
  }



  batchUpsertCache(aDocArray) {
    const action = 'upsert';    

    let NBatchSize = 5000;
    let iStart = 0;
    let iEnd = Math.floor(Math.min(NBatchSize - 1,aDocArray.length));

    const nextBatch = (iStart,iEnd) => {
      let batch = this.client.batch();
      return new Promise( (resolve,reject) => {
        for (let i = iStart;i < iEnd;i++) {
          let oDoc = aDocArray[i];
          const sDocString    = JSON.stringify(oDoc);
          const cacheId       = getCacheId({ id: oDoc.id, cityCode: oDoc.cityCode });
          const cacheChannel  = getCacheChannel({ action: action, cityCode: oDoc.cityCode });
          const sortedSet     = getSortedSetName(oDoc.cityCode);

          batch.set(cacheId, sDocString);
          batch.zadd(sortedSet, oDoc.cs, cacheId); // use created time for ordered set
          batch.publish(cacheChannel, sDocString);
        }
        batch.exec( (err,aResponse) => {
          // console.log('batchUpsertCache', aResponse);
          if (err) {
            reject(err);
          } 
          else {
            resolve(aResponse);
          }
        })
      })
    }

    let aAllResponses = [];
    const runAll = () => {
      return nextBatch(iStart,iEnd)
      .then( aResponse => {
        // append results
        aAllResponses = aAllResponses.concat(aResponse);
        // update batch offset
        iStart = iEnd;
        iEnd = Math.floor(Math.min(iEnd + NBatchSize,aDocArray.length));
        if (iStart < aDocArray.length) {
          return runAll();
        }
        else {
          return aAllResponses;
        }
      })
    }

    return runAll();
  }

  batchRemoveFromCache(aCacheIdArray) {
    const action = 'remove'; // used for publish

    let NBatchSize = 5000;
    let iStart = 0;
    let iEnd = Math.floor(Math.min(NBatchSize - 1,aCacheIdArray.length));

    const nextBatch = (iStart,iEnd) => {
      return new Promise( (resolve,reject) => {
        let batch = this.client.batch();
        for (let i = iStart;i < iEnd;i++) {
          let cacheId     = aCacheIdArray[i];
          const oDocId    = getDocId(cacheId);
          const sortedSet = getSortedSetName(oDocId.cityCode);

          // console.log('batchRemoveFromCache cacheId',cacheId,'oDocId',oDocId);
          batch.del(cacheId);
          batch.zrem(sortedSet, cacheId);
          if (oDocId) {
            const cacheChannel  = getCacheChannel({ cityCode: oDocId.cityCode, action: action });
            // console.log('batchRemoveFromCache cacheChannel',cacheChannel);
            batch.publish(cacheChannel, cacheId);          
          }
        }
        batch.exec( (err,aResponse) => {
          // console.log('batchRemoveCache', aResponse);
          if (err) {
            reject(err);
          } 
          else {
            resolve(aResponse);
          }
        })
      })
    }

    let aAllResponses = [];
    const runAll = () => {
      return nextBatch(iStart,iEnd)
      .then( aResponse => {
        // append results
        aAllResponses = aAllResponses.concat(aResponse);
        // update batch offset
        iStart = iEnd;
        iEnd = Math.floor(Math.min(iEnd + NBatchSize,aCacheIdArray.length));
        if (iStart < aCacheIdArray.length) {
          return runAll();
        }
        else {
          return aAllResponses;
        }
      })
    }

    return runAll();
  }

}

module.exports = {
  subscribeToCache      : subscribeToCache,
  Cache                 : Cache,
  getCacheId            : getCacheId,
  getCacheChannel       : getCacheChannel,
  getDocId              : getDocId,
  getSortedSetName      : getSortedSetName
};

