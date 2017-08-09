const url               = require('url');
const bluebird          = require('bluebird');
const redis             = require('redis');
const cache             = require('./cache');
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
  return `tr:${options.id}`;
}

const getDocId = (cacheId) => {
  const sAction = 'getDocId';
  let   oDocId  = null;
  if (typeof cacheId === 'string') {
    let aResults = cacheId.match(/tr:(.*)/);
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
  return `tr:${options.cityCode}:${options.action}`;
}

const getSortedSetName = (cityCode) => {
  return `tr:${cityCode}:sortedSet`;
}


const subscribeToCache = (options) => {
  return new CacheSubscriber(options);
}

// add city to write/publish actions
// reader, writer, and publisher
class TrendingCache extends cache.Cache {
  constructor(options) {
    super(options);
    cleanupClientOnProcessEnd({ name: 'TrendingCache', client: this.client });    
  }

  getWithDocId(options) {
    const cacheId = getCacheId(options);
    return this.client.getAsync(cacheId).then( sData => {
      return JSON.parse(sData);
    })
  }


  upsert(oDoc) {
    const sAction   = TrendingCache.name + '.upsert';

    const cityCode      = oDoc.cityCode;
    const id            = oDoc.id;
    const action        = 'upsert';
    const cacheId       = getCacheId({ id: id });
    const cacheChannel  = getCacheChannel({ action: action, cityCode: cityCode });
    const sortedSet     = getSortedSetName(cityCode);

    if (typeof oDoc.cs !== 'number') {
      return Promise.reject(Error(`${sAction}.invalid.doc.err invalid or missing cs property, oDoc ${JSON.stringify(oDoc)}`))
    }

    if (typeof oDoc.level !== 'number') {
      oDoc.level = 0;
    }

    // console.log({ action: 'Cache.upsert.debug', oDocCs: oDoc.cs, cacheId: cacheId, sortedSet: sortedSet })

    let aPromises = [];
    aPromises.push(this.client.setAsync(cacheId, JSON.stringify(oDoc)));
    // pass in trending score oDoc.score
    aPromises.push(this.client.zaddAsync(sortedSet, oDoc.score, cacheId));

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
    oCacheIncident.score      = oCacheIncident.score;
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
  
  // cacheId: 'tr:$docId' }
  removeCacheId(cacheId) {
    const sAction = TrendingCache.name + '.removeCacheId';
    const oDocId  = getDocId(cacheId);
    const action  = 'remove';

    return this.getWithCacheId(cacheId).then( oDoc => {
      const cityCode = oDoc.cityCode;
      let aPromises = [];
      aPromises.push(this.client.delAsync(cacheId));
      if (typeof cityCode === 'string' && cityCode.length > 0) {
        const cacheChannel  = getCacheChannel({ cityCode: cityCode, action: action });
        const sortedSet     = getSortedSetName(cityCode);

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
    const cacheId  = getCacheId({ id: oDocId });
    
    return this.removeCacheId(cacheId);
  }

  batchGetFromCache(options) {
    const sAction = TrendingCache.name + '.batchGetFromCache';
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

  batchUpsertCache(options) {
    const action          = 'upsert'; 
    const aDocArray       = options.aDocArray;
    const cityCode        = options.cityCode;   
    const bIsDocArray     = Array.isArray(aDocArray);
    const bValidCityCode  = typeof cityCode === 'string' && cityCode.length > 0;
    const bValidOptions   = bIsDocArray && bValidCityCode;
    if (!bValidOptions) {
      return Promise.reject(Error(`TrendingCache.batchUpserCache.invalid.inputs.err aDocArray? ${bIsDocArray} cityCode ${cityCode}`))
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
          const cacheId       = getCacheId({ id: oDoc.id });
          batch.set(cacheId, sDocString);

          const cityCode      = oDoc.cityCode;
          // don't add to sorted set or publish events without a cityCode
          if (typeof cityCode === 'string' && cityCode.length > 0 && typeof oDoc.score === 'number') {
            const cacheChannel  = getCacheChannel({ action: action, cityCode: oDoc.cityCode });
            const sortedSet     = getSortedSetName(oDoc.cityCode);

            batch.zadd(sortedSet, oDoc.score, cacheId); // use created time for ordered set
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
    const sAction         = TrendingCache.name + '.batchRemoveFromCache';    
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
          const oDocId    = getDocId(cacheId);
          const sortedSet = getSortedSetName(cityCode);

          // console.log('batchRemoveFromCache cacheId',cacheId,'oDocId',oDocId);
          batch.del(cacheId);
          batch.zrem(sortedSet, cacheId);
          if (oDocId) {
            const cacheChannel  = getCacheChannel({ cityCode: cityCode, action: action });
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


