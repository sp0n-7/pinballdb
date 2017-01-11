const url               = require('url');
const bluebird          = require('bluebird');
const redis             = require('redis');
bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(redis.Multi.prototype);

const Promise           = bluebird;


const sRedisUrl = 'redis://localhost:6379';

// helper functions
const getRedisClient = (sUrl) => {
  const redisUrl  = url.parse(sUrl);
  const client    = redis.createClient(redisUrl.port, redisUrl.hostname);
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
  let   oDocId  = { city: null, id: null };
  if (typeof cacheId === 'string') {
    let aResults = cacheId.match(/pb:(.*):(.*)/);
    if (Array.isArray(aResults) && aResults.length >= 3) {
      oDocId.city = aResults[1];
      oDocId.id   = aResults[2];
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

const oChannels = {
  upsert: 'upsert',
  remove: 'remove'
}

// note:
// redis psubscribe and pmessage is kinda nutz
// it emits disconnected keyspace and keyevent pmessages
// manually publish and subscribe instead


class Cache {
  constructor(options) {
    // this.sClass = 'Cache';
    this.client = getRedisClient(options.sCacheUrl);
  }

  getWithCacheId(cacheId) {
    return this.client.getAsync(cacheId).then( sData => {
      return JSON.parse(sData);
    })
  }

  getWithDocId(options) {
    const cacheId = getCacheId(options);
    return this.client.getAsync(cacheId).then( sData => {
      return JSON.parse(sData);
    })
  }

}

// add city subscribe actions
class CacheSubscriber extends Cache {
  
  // options { sUrl: sRedisUrl, oProcs:{ upsert: fUpsert, remove: fRemove }}
  // returns a Promise which resolves after subscriptions are set
  // new CacheSubscriber(options).then( (instance) => { ... })
  constructor(options) { 
    const sAction = CacheSubscriber.name + '.constructor';

    super(options);
    this.oProcs     = options.oProcs;
    const aChannels = Object.keys(this.oProcs);
    console.log('aChannels',aChannels);

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
class CacheWriter extends Cache {
  constructor(options) {
    super(options);
  }

  upsert(oDoc) {
    const cityCode      = oDoc.cityCode;
    const id            = oDoc.id;
    const action        = 'upsert';
    const cacheId       = getCacheId({ id: id, cityCode: cityCode});
    const cacheChannel  = getCacheChannel({ action: action, cityCode: cityCode });
    
    return this.client.setAsync(cacheId, JSON.stringify(oDoc)).then( (res) => {
      return this.client.publishAsync(cacheChannel, JSON.stringify(oDoc)).then( () => {
        return res;
      })
    });
  }

  // cacheId: 'pb:$cityCode:$docId' }
  removeCacheId(cacheId) {
    const oDocId        = getDocId(cacheId);
    const action        = 'remove';
    const cacheChannel  = getCacheChannel({ cityCode: oDocId.cityCode, action: action });
    return this.client.delAsync(cacheId).then( (res) => {
      return this.client.publishAsync(cacheChannel, cacheId).then( () => {
        return res;
      })
    });
  }

  // { id: $docId, cityCode: $cityCode }
  removeDoc(options) {
    const oDocId        = options.id;
    const cityCode      = options.cityCode;
    const action        = 'remove';
    const cacheId       = getCacheId({ id: oDocId, cityCode: cityCode });
    const cacheChannel  = getCacheChannel({ cityCode: cityCode, action: action });
    return this.client.delAsync(cacheId).then( (res) => {
      return this.client.publishAsync(cacheChannel, cacheId).then( () => {
        return res;
      })
    });    
  }

  batchUpsertCache(aDocArray) {
    return new Promise( (resolve,reject) => {
      let batch = this.client.batch();
      for (let i = 0;i < aDocArray.length;i++) {
        let oDoc = aDocArray[i];
        const sDocString = JSON.stringify(oDoc);
        batch.set(oDoc.id, sDocString);
        batch.publish('upsert', sDocString);
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

  batchRemoveFromCache(aCacheIdArray) {
    const action = 'remove'; // used for publish
    return new Promise( (resolve,reject) => {
      let batch = this.client.batch();
      for (let i = 0;i < aCacheIdArray.length;i++) {
        let cacheId   = aCacheIdArray[i];
        const oDocId  = getDocId(cacheId);

        batch.del(cacheId);
        const cacheChannel  = getCacheChannel({ cityCode: oDocId.cityCode, action: action });
        batch.publish(cacheChannel, cacheId);
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

}

module.exports = {
  subscribeToCache      : subscribeToCache,
  CacheWriter           : CacheWriter,
  getCacheId            : getCacheId,
  getCacheChannel       : getCacheChannel,
  getDocId              : getDocId
};

