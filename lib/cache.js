const url               = require('url');
const bluebird          = require('bluebird');
const redis             = require('redis');
bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(redis.Multi.prototype);
const u                 = require('./util')

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
  return `pb:${options.id}`;
}

const getDocId = (cacheId) => {
  const sAction = 'getDocId';
  let   oDocId  = null;
  if (typeof cacheId === 'string') {
    let aResults = cacheId.match(/pb:(.*)/);
    if (Array.isArray(aResults) && aResults.length >= 2) {
      oDocId = { id: aResults[1] }
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

const cleanupClientOnProcessEnd = (options) => {
  const sAction       = 'cache.cleanupClientOnProcessEnd';
  const name          = options.name;
  let client          = options.client;
  const killDelayTime = 500;

  const killClient = (sType) => {
    console.info({action: `${name}.${sAction}.${sType}.client.quit` });
    client.quit();
    
    setTimeout( () => {
      client.end(true);
      console.info({action: `${name}.${sAction}.${sType}.client.end` });
      if (sType === 'SIGINT' || sType == 'SIGTERM') {
        process.exit(0);
      }
    },killDelayTime);    
  }
  
  process.on( 'SIGINT', () => {
    killClient('SIGINT');
  });    

  process.on( 'SIGTERM', () => {
    killClient('SIGTERM');
  });

  process.on('exit', (code) => {
    killClient('exit code' + code);
  });
}


// note:
// redis psubscribe and pmessage is kinda nutz
// it emits disconnected keyspace and keyevent pmessages
// manually publish and subscribe instead



class CacheBase {

  constructor(options) {
    // this.sClass = 'Cache';

    if (!u.validString(options.sCacheUrl) || !u.validString(options.setName) || !u.validString(options.scoreProperty)) {
      throw Error(`CacheBase.constructor.err invalid inputs options ${JSON.stringify(options)}`)
    }

    this.client = getRedisClient(options.sCacheUrl);
    this.setName = options.setName
    this.scoreProperty = options.scoreProperty
  }
  
  getCacheId(options){
    return `${this.setName}:${options.id}`;
  }

  getDocId(cacheId){
    const sAction = 'getDocId';
    let   oDocId  = null;
    if (typeof cacheId === 'string') {
      let regEx  = new RegExp(`${this.setName}:(.*)`)
      let aResults = cacheId.match(regEx);
      if (Array.isArray(aResults) && aResults.length >= 2) {
        oDocId = { id: aResults[1] }
      }
      else {
        console.error({ action: sAction + '.err', cacheId: cacheId })
      }
    }
    return oDocId;
  }

  // currently same format as getCacheId 
  getCacheChannel(options){
    return `${this.setName}:${options.cityCode}:${options.action}`;
  }

  getSortedSetName(cityCode){
    return `${this.setName}:${cityCode}:sortedSet`;
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
    cleanupClientOnProcessEnd({ name: 'CacheSubscriber', client: this.client });

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
    cleanupClientOnProcessEnd({ name: 'Cache', client: this.client });    
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
    const cacheId = this.getCacheId(options);
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
    const sAction   = Cache.name + '.upsert';

    const cityCode      = oDoc.cityCode;
    const id            = oDoc.id;
    const action        = 'upsert';
    const cacheId       = this.getCacheId({ id: id });
    const cacheChannel  = this.getCacheChannel({ action: action, cityCode: cityCode });
    const sortedSet     = this.getSortedSetName(cityCode);

    if (typeof oDoc[this.scoreProperty] !== 'number') {
      return Promise.reject(Error(`${sAction}.invalid.doc.err invalid or missing cs property, oDoc ${JSON.stringify(oDoc)}`))
    }

    if (typeof oDoc.level !== 'number') {
      oDoc.level = 0;
    }

    // console.log({ action: 'Cache.upsert.debug', oDocCs: oDoc[this.scoreProperty], cacheId: cacheId, sortedSet: sortedSet })

    let aPromises = [];
    aPromises.push(this.client.setAsync(cacheId, JSON.stringify(oDoc)));
    aPromises.push(this.client.zaddAsync(sortedSet, oDoc[this.scoreProperty], cacheId));

    return Promise.all(aPromises).then( aRes => {
      return this.client.publishAsync(cacheChannel, JSON.stringify(oDoc)).then( () => {
        return aRes[0];
      })      
    })    
  }

  upsertFromIncident(options) {
    const id                  = options.id;
    const oIncident           = options.val;
    const cityCode            = oIncident.cityCode;
    if (typeof cityCode !== 'string' || cityCode.length == 0) {
      return Promise.reject(Error(`Cache.upsertFromIncident.err invalid cityCode, oIncident ${JSON.stringify(oIncident)}`))
    }

    const oCacheIncident      = Object.assign({},oIncident);
    oCacheIncident.id         = id;
    oCacheIncident.score      = oCacheIncident[this.scoreProperty];
    if (typeof oIncident === 'object' && 
        Array.isArray(oIncident.ll)   && oIncident.ll.length == 2) 
    {
      oCacheIncident.latitude  = oIncident.ll[0];
      oCacheIncident.longitude = oIncident.ll[1];      
    }
    if (typeof oCacheIncident.level !== 'number') {
      oCacheIncident.level = 1;
    }
    return this.upsert(oCacheIncident);
  } 
  
  // cacheId: 'pb:$docId' }
  removeCacheId(cacheId) {
    const sAction = Cache.name + '.removeCacheId';
    const oDocId  = this.getDocId(cacheId);
    const action  = 'remove';

    return this.getWithCacheId(cacheId).then( oDoc => {
      const cityCode = oDoc.cityCode;
      let aPromises = [];
      aPromises.push(this.client.delAsync(cacheId));
      if (typeof cityCode === 'string' && cityCode.length > 0) {
        const cacheChannel  = this.getCacheChannel({ cityCode: cityCode, action: action });
        const sortedSet     = this.getSortedSetName(cityCode);

        let pCleanUpAndPublish = this.client.zremAsync(sortedSet, cacheId).then( res => {
          return this.client.publishAsync(cacheChannel, cacheId).then( () => {
            return res;
          });
        });
        aPromises.push(pCleanUpAndPublish);
      }
      else {
        console.error({ action: sAction + '.get.cityCode.err', cacheId: cacheId, oDoc: JSON.stringify(oDoc)})
      }
      return Promise.all(aPromises);
    })
  }

  // { id: $docId }
  removeDoc(options) {
    const oDocId   = options.id;
    const action   = 'remove';
    const cacheId  = this.getCacheId({ id: oDocId });
    
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


  batchUpsertCache(options) {
    const action          = 'upsert'; 
    const aDocArray       = options.aDocArray;
    const cityCode        = options.cityCode;   
    const bIsDocArray     = Array.isArray(aDocArray);
    const bValidCityCode  = typeof cityCode === 'string' && cityCode.length > 0;
    const bValidOptions   = bIsDocArray && bValidCityCode;
    if (!bValidOptions) {
      return Promise.reject(Error(`Cache.batchUpserCache.invalid.inputs.err aDocArray? ${bIsDocArray} cityCode ${cityCode}`))
    }

    let NBatchSize = 5000;
    let iStart = 0;
    let iEnd = Math.floor(Math.min(NBatchSize - 1,aDocArray.length));

    const nextBatch = (iStart,iEnd) => {
      let batch = this.client.batch();
      return new Promise( (resolve,reject) => {
        for (let i = iStart;i < iEnd;i++) {
          let oDoc            = aDocArray[i];
          const sDocString    = JSON.stringify(oDoc);
          const cacheId       = this.getCacheId({ id: oDoc.id });
          batch.set(cacheId, sDocString);

          const cityCode      = oDoc.cityCode;
          // don't add to sorted set or publish events without a cityCode
          if (typeof cityCode === 'string' && cityCode.length > 0 && typeof oDoc[this.scoreProperty] === 'number') {
            const cacheChannel  = this.getCacheChannel({ action: action, cityCode: oDoc.cityCode });
            const sortedSet     = this.getSortedSetName(oDoc.cityCode);

            batch.zadd(sortedSet, oDoc[this.scoreProperty], cacheId); // use created time for ordered set
            batch.publish(cacheChannel, sDocString);
          }
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

  batchRemoveFromCache(options) {
    const sAction         = Cache.name + '.batchRemoveFromCache';    
    const action          = 'remove'; // used for publish
    const aCacheIdArray   = options.aCacheIds;
    const cityCode        = options.cityCode;
    const bCacheIsArray   = Array.isArray(aCacheIdArray);
    const bValidCityCode  = typeof cityCode === 'string' && cityCode.length;
    const bValidOptions   = bCacheIsArray && bValidCityCode;
    
    if (!bValidOptions) {
      const err = new Error(`${sAction}.invalid.options.err aCacheIdArray? ${bCacheIsArray} bValidCityCode? ${bValidCityCode}`)
      return Promise.reject(err);
    }

    let NBatchSize = 5000;
    let iStart = 0;
    let iEnd = Math.floor(Math.min(NBatchSize - 1,aCacheIdArray.length));

    const nextBatch = (iStart,iEnd) => {
      return new Promise( (resolve,reject) => {
        let batch = this.client.batch();
        for (let i = iStart;i < iEnd;i++) {
          const cacheId   = aCacheIdArray[i];
          const oDocId    = this.getDocId(cacheId);
          const sortedSet = this.getSortedSetName(cityCode);

          // console.log('batchRemoveFromCache cacheId',cacheId,'oDocId',oDocId);
          batch.del(cacheId);
          batch.zrem(sortedSet, cacheId);
          if (oDocId) {
            const cacheChannel  = this.getCacheChannel({ cityCode: cityCode, action: action });
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

