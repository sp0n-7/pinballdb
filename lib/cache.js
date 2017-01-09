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

class Cache {
  constructor(options) {
    // this.sClass = 'Cache';
    this.client = getRedisClient(options.sUrl);
  }
}

// add city to publish and subscribe actions

class CacheSubscriber extends Cache {
  // options { sUrl: sRedisUrl, oProcs:{ upsert: fUpsert, remove: fRemove }}
  constructor(options) {
    this.sClass = 'CacheSubscriber';
    const sAction = this.sClass + '.constructor';
    super(options);
    this.oProcs = options.oProcs;

    for (let i in oChannels) {
      this.client.subscribe(oChannels[i]);
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
      console.log({ action: sAction + '.on.message', channel: channel, message: messageToSend });
      let fProc = oProcs[channel];
      if (typeof fProc === 'function') {
        fProc(messageToSend)
      }
    });
  }
}

// add city to publish and subscribe actions

class CachePublisher extends Cache {
  constructor(options) {
    // this.sClass = 'CachePublisher';
    // const sAction = this.sClass + '.constructor';
    super(options);
  }

  const upsertDoc = (oDoc) => {
    return this.client.hmsetAsync(oDoc.id, oDoc).then( (res) => {
      this.client.publish('upsert', JSON.stringify(oDoc));
      return res;
    });
  }

  const removeDoc = (id) => {
    return this.getDoc(id).then( (oDoc) => {
      let aKeys = Object.keys(oDoc);
      return this.client.hdelAsync(id,aKeys).then( (res) => {
        this.client.publish('remove', id);
        return res;
      });
    });
  }

  const getDoc = (id) => {
    return this.client.hgetallAsync(id);
  }
}

module.exports = {
  Cache: Cache
};

