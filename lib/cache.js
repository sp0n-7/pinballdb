const url               = require('url');
const bluebird          = require('bluebird');
const redis             = require('redis');
bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(redis.Multi.prototype);

const sRedisUrl = 'redis://localhost:6379';

const getRedisClient = (sUrl) => {
  const redisUrl  = url.parse(sUrl);
  const client    = redis.createClient(redisUrl.port, redisUrl.hostname);
  // console.log({ action: 'app', redisUrl: redisUrl });
  if (redisUrl.auth) {
    client.auth(redisUrl.auth.split(":")[1]);
  }
  return client;
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

  get(id) {
    // works with hmset, which doesn't work well with nested objects
    // return this.client.hgetallAsync(id);
    return this.client.getAsync(id).then( sData => {
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
    this.oProcs = options.oProcs;

    let aSubscribePromises = [];
    for (let i in oChannels) {
      const sChannel = oChannels[i];
      aSubscribePromises.push(this.client.subscribeAsync(sChannel));

      // const subPromise = new Promise( (resolve,reject) => {
      //   this.client.on('subscribe', function (channel, count) {
      //     // console.log({ action: sAction + '.subPromise', channel: channel, count: count });
      //     if (channel === sChannel) {
      //       resolve(this);
      //     }
      //   });                            
      // })

      // aSubscribePromises.push(subPromise)
    }

    this.client.on('message', (channel, message) => {
      let messageToSend = message;
      if (channel === oChannels.upsert) {
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
    // this.sClass = 'CacheWriter';
    // const sAction = this.sClass + '.constructor';
    super(options);
  }

  upsert(oDoc) {
    return this.client.setAsync(oDoc.id, JSON.stringify(oDoc)).then( (res) => {
      return this.client.publishAsync('upsert', JSON.stringify(oDoc)).then( () => {
        return res;
      })
    });
  }

  remove(id) {
    return this.client.delAsync(id).then( (res) => {
      return this.client.publishAsync('remove', id).then( () => {
        return res;
      })
    });
  }

  batchUpsertCache(aDocArray) {
    return Promise.each(aDocArray, this.upsert.bind(this));
  }

  batchRemoveFromCache(aIdArray) {
    return Promise.each(aIdArray, this.remove.bind(this));
  }

}

module.exports = {
  subscribeToCache      :  subscribeToCache,
  CacheWriter           :  CacheWriter
};

