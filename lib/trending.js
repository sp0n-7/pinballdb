const u                 = require('./util');
const binarySearch      = u.binarySearch;
const findIndex         = u.findIndex;
const cache             = require('./cache');
const Cache             = cache.Cache;
const CacheSubscriber   = cache.CacheSubscriber;

class Trending {
  constructor(options) {
    this.db       = {}; // kv in memory store

    // an object which includes which bucket each object falls within ie this.meta[id].aBucketIndex
    this.meta     = {}; 

    // global ordered by score array
    this.aOrdered = [];

    this.cityCode = options.cityCode;

    // total region covered lat & lon
    this.lowerLatitude  = options.lowerLatitude;
    this.upperLatitude  = options.upperLatitude;
    this.lowerLongitude = options.lowerLongitude;
    this.upperLongitude = options.upperLongitude;

    // grid dimensions
    this.NLatitude      = options.NLatitude;
    this.NLongitude     = options.NLongitude;

    // grid resolution lat, lon: grid footprint not constant area
    this.deltaLatitude  = (this.upperLatitude  - this.lowerLatitude ) / this.NLatitude;
    this.deltaLongitude = (this.upperLongitude - this.lowerLongitude) / this.NLongitude;

    // input validation
    if (typeof this.cityCode  !== 'string'      || this.cityCode.length       === 0        ||
        typeof this.lowerLatitude !== 'number'  || typeof this.upperLatitude  !== 'number' ||
        typeof this.lowerLongitude !== 'number' || typeof this.upperLongitude !== 'number' ||
        typeof this.NLatitude !== 'number'      || typeof this.NLongitude     !== 'number')
    {
      throw Error(`${Trending.name}.constructor invalid options: ${JSON.stringify(options)}`);
    }    

    // grid, NLongitude by NLatitude buckets
    // each bucket is an object of { id: { latitude: latitude, longitude: longitude, val: val }, ... }
    this.grid = []
    this.grid.length = this.NLongitude;
    for (let i = 0; i < this.NLongitude;i++) {
      this.grid[i] = [];
      this.grid[i].length = this.NLatitude;
      for (let j = 0; j < this.NLatitude;j++) {
        this.grid[i][j] = {
          aArray : [],
          nBucketTotal : 0
        }; 
      }
    }

    this.offGridBucket = {
      aArray : [],
      nBucketTotal : 0    
    }
  }

  // options: { longitude: longitude, latitude: latitude }
  _getBucketIndexes(options) {
    // check if outside range
    if (options.longitude < this.lowerLongitude || options.longitude > this.upperLongitude || 
        options.latitude  < this.lowerLatitude  || options.latitude  > this.upperLatitude) 
    {
      return null;
    }
    const iLongitude = Math.floor( (options.longitude - this.lowerLongitude) / this.deltaLongitude);
    const iLatitude  = Math.floor( (options.latitude  - this.lowerLatitude ) / this.deltaLatitude );
    return [iLongitude,iLatitude];
  }

  _getLimitedBucketIndexes(options) {

    let iLongitude = Math.floor( (options.longitude - this.lowerLongitude) / this.deltaLongitude);
    if (iLongitude < 0) iLongitude = 0;
    else if (iLongitude >= this.NLongitude - 1 ) iLongitude = this.NLongitude - 1;

    let iLatitude  = Math.floor( (options.latitude  - this.lowerLatitude ) / this.deltaLatitude );
    if (iLatitude < 0) iLatitude = 0;
    else if (iLatitude >= this.NLatitude - 1 ) iLatitude = this.NLatitude - 1;

    return [iLongitude,iLatitude];
  }

  // options: { id:id, longitude: longitude, latitude: latitude, val: val }
  _addToBucket(options) {
    const sAction = Trending.name + '._addToBucket';
    const aBI   = this._getBucketIndexes(options);
    const id    = options.id;
    const val   = options.val; 

    if (aBI) {

      const i = aBI[0];
      const j = aBI[1];
      if (i < 0 || i >= this.NLongitude || j < 0 || j >= this.NLatitude) {
        console.error({ action: sAction + '.out.of.bounds.err', id: id, 
          iLongitude: i, jLatitude: j, NLon: this.NLongitude, NLat: this.NLatitude });
        return;
      }

      this.grid[i][j].aArray.push({ id: id, latitude: options.latitude, longitude: options.longitude, val: options.val, level: options.level });
      this.grid[i][j].nBucketTotal++;

      // update meta
      this.meta[id] = { aBucketIndex : [i,j] };
    }
    else {
      this.offGridBucket.aArray.push({ id: id, latitude: options.latitude, longitude: options.longitude, val: options.val, level: options.level });
      this.offGridBucket.nBucketTotal++;

      // update meta
      this.meta[id] = { bOffGrid : true };

    }
  }  

  // deletions are slow, needs to scan through grid aArrays for id
  _removeFromBucket(id) {
    const sAction = Trending.name + '._removeFromBucket';
    const oMeta = this.meta[id];
    if (oMeta) {
      const aStoredBI = oMeta.aBucketIndex;    
      if (aStoredBI) {
        const i = aStoredBI[0];
        const j = aStoredBI[1];
        // consistency check
        if (i < 0 || i >= this.NLongitude || j < 0 || j >= this.NLatitude) {
          console.error({ action: sAction + '.out.of.bounds.err', id: id, 
            iLongitude: i, jLatitude: j, NLon: this.NLongitude, NLat: this.NLatitude });
          return;
        }

        let aArray = this.grid[i][j].aArray;
        for (let k = 0;k < aArray.length;k++) {
          let oData = aArray[k];
          if (oData && oData.id === id) {
            aArray[k] = null;
            break;
          }
        }
        this.grid[i][j].nBucketTotal--;
        this.meta[id] = null;
      }
      else if (oMeta.bOffGrid) {
        let aArray = this.offGridBucket.aArray;
        for (let k = 0;k < aArray.length;k++) {
          let oData = aArray[k];
          if (oData && oData.id === id) {
            aArray[k] = null;
            break;
          }
        }
        this.offGridBucket.nBucketTotal--;
        this.meta[id] = null;        
      }      
    }
  }  


  _addToData(options) {
    this._addToBucket({ 
      id: options.id, 
      longitude: options.longitude, 
      latitude: options.latitude, 
      val: options.score,
      level: options.level
    });
  }

  _removeFromData(id) {
    const oOldDoc = this.db[id];
    if (oOldDoc) {
      this._removeFromBucket(id);
    }
  }

  printGrid() {
    for (let i = 0; i < this.NLongitude;i++) {
      let aRow = [];
      for (let j = 0; j < this.NLatitude;j++) {
        aRow.push(this.grid[i][j].nBucketTotal)
      }
      console.log(aRow.join(' '));
    }
    console.log('offGrid',this.offGridBucket.nBucketTotal)
  }

  // will only come from redis subscriber

  // oDoc's key is oDoc.id
  upsert(oDoc) {
    // if valid

    // ensure latitude and longitude are set properties
    if (typeof oDoc.latitude !== 'number' && oDoc.ll && typeof oDoc.ll[0] === 'number') {
      oDoc.latitude       = oDoc.ll[0];
    }
    if (typeof oDoc.longitude !== 'number' && oDoc.ll && typeof oDoc.ll[0] === 'number') {
      oDoc.longitude      = oDoc.ll[1];
    }
 
    if (typeof oDoc.score !== 'number') {
      throw Error (`trending.upsert.invalid.doc ${JSON.stringify(oDoc)}`)
    }
    if (typeof oDoc.id === 'string' & oDoc.id.length > 0 && 
        typeof oDoc.longitude === 'number' && 
        typeof oDoc.latitude  === 'number' && 
        typeof oDoc.score     === 'number') 
    {
      const oPartialDoc = { id: oDoc.id, score: oDoc.score, latitude: oDoc.latitude, longitude: oDoc.longitude, ts: oDoc.ts, cs: oDoc.cs, level: oDoc.level };

      // if object id is already in db update
      if (this.db[oDoc.id]) {
        this._removeFromBucket(oDoc.id);
        this._addToBucket({ 
          id: oDoc.id, 
          longitude: oDoc.longitude, 
          latitude: oDoc.latitude, 
          val: oDoc.score,
          level: oDoc.level
        });
      }
      else {
        this._addToData(Object.assign({},oPartialDoc));        
      }
      // this.db[oDoc.id] = oPartialDoc;
      this.db[oDoc.id] = oDoc;
    }
    else {
      console.info({ action: 'upsert.skipping.oDoc', oDoc: oDoc })
    }
  }

  batchUpsert(batch) {
    for (let oDoc of batch) {
      this.upsert(oDoc)
    }
  }


  remove(id) {
    if (this.db[id]) {
     this._removeFromData(id);
     this.db[id] = null;
    }
  }

  // load from and subscribe to remote cache for 
  // tr:$cityCode:upsert and tr:$cityCode:remove channel messages
  // after subscription, keeps connection to cacheDB for future use
  addSubscriber(options) {
    const sAction   = Trending.name + '.addSubscriber'; 
    const sCacheUrl = options.sCacheUrl;
    const aProps    = options.aProps;
    const cityCode  = this.cityCode;

    if (typeof sCacheUrl !== 'string'  || sCacheUrl.length === 0) 
    {
      return Promise.reject(Error(`${sAction}.input.err options ${JSON.stringify(options)}`))
    }    

    this.sCacheUrl = sCacheUrl;

    const oSubscriberOptions =  { 
      sCacheUrl     : sCacheUrl,
      setName       : 'tr',
      scoreProperty : 'trendingScore'
    }
    this.cacheSubscriber = new CacheSubscriber(oSubscriberOptions)

    let   oProcs    = {};

    oProcs[`tr:${cityCode}:upsert`] = (oDoc) => {
      // console.log({ action: 'subscriber.proc.upsert', oDoc:oDoc });
      this.upsert(oDoc);
    }

    oProcs[`tr:${cityCode}:remove`] = (cacheId) => {
      const oDocId = this.cacheSubscriber.getDocId(cacheId);
      // console.log({ action: 'subscriber.proc.remove', cacheId: cacheId });
      if (oDocId != null && u.validString(oDocId.id)) {
        this.remove(oDocId.id);
      }
      else {
        console.error({ action: `oProcs.tr.${cityCode}.remove.err`, cacheId: cacheId })
      }
    }

    // this will miss updates since beginning of load
    // return this.loadFromCache({ sCacheUrl: sCacheUrl }).then( () => {
    //   return cache.subscribeToCache(oSubscriberOptions);
    // })

    // possible issue
    // after subscription is setup but before local pinball imports all data redis
    // need to queue new changes, then apply them to imported data
    return this.cacheSubscriber.subscribe(oProcs).then( () => {
      return this.loadFromCache({ sCacheUrl: sCacheUrl, aProps: aProps });
    })
  }

  // also keeps a connection to the cacheDB for future use
  loadFromCache(options) {
    const sAction      = Trending.name + '.loadFromCache';
    const sCacheUrl    = options.sCacheUrl;
    const aProps       = options.aProps;
    const cityCode     = this.cityCode;

    // pinball now has a rw cacheDB
    this.cacheDB       = new Cache({ sCacheUrl : sCacheUrl, setName:'tr', scoreProperty: 'trendingScore'  });
    
    const t0 = Date.now();
    let t1;

    const scanPattern = `tr:${cityCode}:*`; // used for keys
   // const setKey      = cache.getSortedSetName(cityCode);
    
    const setKey      = this.cacheDB.getSortedSetName(cityCode)
    console.log({ action: sAction, cityCode: cityCode, setKey: setKey });
    return this.cacheDB.orderedKeys({ setKey: setKey })
    .then( aKeys => {
      t1 = Date.now();
      console.log({ action: sAction + '.scanned.keys',  cityCode: cityCode, length: aKeys.length, time: t1-t0 });
      let oBatchGetOptions = { aCacheIds: aKeys };
      return this.cacheDB.batchGetFromCache({ aCacheIds: aKeys, aProps: aProps });
    })
    .then( aObjects => {
      let t2 = Date.now();
      console.log({ action: sAction + '.batchGetFromCache', cityCode: cityCode, length: aObjects.length, time:t2-t1 });
      // upserts to new tr zSet
      //return this.cacheDB.batchUpsertCache({ aDocArray: aObjects, cityCode: cityCode })
      for (let i=0;i < aObjects.length;i++) {
        console.log(aObjects[i].id,aObjects[i].cs)
        this.upsert(aObjects[i]);
      }
      aObjects = [];
      let t3 = Date.now();
      console.log({ action: sAction + '.finishedUpsert', cityCode: cityCode, length: this.aOrdered.length, time:t3-t2 });
    })
  }

  // returns Promise, async gets full json from cache
  queryAsync(options) {
    const aPartialResults = this.query(options);
    const aCacheIds = aPartialResults.map( oResult => this.cacheSubscriber.getCacheId({ id: oResult.id }));
    return this.cacheDB.batchGetFromCache({ aCacheIds : aCacheIds });
  }

  query(options) {

    let aResults  = [];
    
    const N               = options.N;

    if (N <= 0) {
      return [];
    }

    const lowerLatitude     = options.lowerLatitude;
    const upperLatitude     = options.upperLatitude;
    const lowerLongitude    = options.lowerLongitude;
    const upperLongitude    = options.upperLongitude;

    const matchingEvent = (oData) => {
      const level = typeof oData.level === 'number' ? oData.level : 1;
      return oData.latitude  > lowerLatitude  && oData.latitude  < upperLatitude  &&
             oData.longitude > lowerLongitude && oData.longitude < upperLongitude  
    }

    // console.time('totalFullScan');
    // console.time('totalEmpty');
    // console.time('totalGrid')
    
    // if query overlaps with db
    const bOutsideBounds  = this.lowerLatitude  > upperLatitude  || this.upperLatitude < lowerLatitude || 
                            this.lowerLongitude > upperLongitude || this.upperLongitude < lowerLongitude;

    if (!bOutsideBounds) {

      const aLLIndexes = this._getLimitedBucketIndexes({ longitude: options.lowerLongitude, latitude: options.lowerLatitude });
      const aURIndexes = this._getLimitedBucketIndexes({ longitude: options.upperLongitude, latitude: options.upperLatitude });
      
      let   aBuckets   = [];
      let   nTotal     = 0;

      // ilongitude loop
      if (aLLIndexes && aURIndexes) {

        // console.time('bucketCount');
        for (let i = aLLIndexes[0];i <= aURIndexes[0];i++) {
          // ilatitude loop
          for (let j = aLLIndexes[1];j <= aURIndexes[1];j++) {

            let oDataSet = this.grid[i][j];
            nTotal += oDataSet.nBucketTotal;
            aBuckets.push(oDataSet.aArray);
          }
        }

        // if query bounds go beyond grid have to add off grid bucket
        if (lowerLatitude  < this.lowerLatitude  || upperLatitude  > this.upperLatitude ||
            lowerLongitude < this.lowerLongitude || upperLongitude > this.upperLongitude)
        {
          aBuckets.push(this.offGridBucket.aArray);
        }

        // console.timeEnd('bucketCount');

        // console.time('loadAndSortArray');
        let aArray = [];
        for (let i = 0; i < aBuckets.length;i++) {
          let aDataSet = aBuckets[i];
          for (let j = 0;j < aDataSet.length;j++) {
            const oData = aDataSet[j];
            // skip null members
            if (oData && matchingEvent(oData)) {
                aArray.push(oData);
            }
          }
        }
        //console.log({aArray:aArray})
        if( aArray.length !== 0){
          aArray.sort( (a,b) => a.val - b.val );
        }

        for (let i = aArray.length - 1;i > -1;i--) {
          const oData = this.db[aArray[i].id];
          aResults.push(oData);
          if (aResults.length == N) {
            break;
          }
        }
        // console.timeEnd('loadAndSortArray');
        // console.timeEnd('totalGrid');
        //console.log({aResults:aResults})
        return aResults;

      }  
      
    }
    else {
      // check off grid
      let aArray = [];
      const aDataSet = this.offGridBucket.aArray;
      for (let j = 0;j < aDataSet.length;j++) {
        const oData = aDataSet[j];
        // skip null members
        if (oData && matchingEvent(oData)) {
          aArray.push(oData);
        }
      }

      aArray.sort( (a,b) => a.val - b.val );

      for (let i = aArray.length - 1;i > -1;i--) {
        const oData = this.db[aArray[i].id];
        aResults.push(oData);
        if (aResults.length == N) {
          break;
        }
      }
      // console.timeEnd('loadAndSortArray');
      // console.timeEnd('totalGrid');
      return aResults;      
    }
    // console.timeEnd('totalEmpty');
    return aResults;
  }

}

module.exports = Trending;

