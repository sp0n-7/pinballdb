const u                       = require('./util');
const binarySearch            = u.binarySearch;
const binarySearchDescending  = u.binarySearchDescending
const findIndex               = u.findIndex;
const cache                   = require('./cache');
const Cache                   = cache.Cache;
const CacheSubscriber         = cache.CacheSubscriber;


class Pinball {
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

    // algorithm switch
    this.NBucketThreshold = options.NBucketThreshold;

    // input validation
    if (typeof this.cityCode  !== 'string'      || this.cityCode.length       === 0        ||
        typeof this.lowerLatitude !== 'number'  || typeof this.upperLatitude  !== 'number' ||
        typeof this.lowerLongitude !== 'number' || typeof this.upperLongitude !== 'number' ||
        typeof this.NLatitude !== 'number'      || typeof this.NLongitude     !== 'number')
    {
      throw Error(`${Pinball.name}.constructor invalid options: ${JSON.stringify(options)}`);
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
    const sAction = Pinball.name + '._addToBucket';
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
    const sAction = Pinball.name + '._removeFromBucket';
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

    // options: { aArray: aArray, id:id, val: val }
  _addToArray(options) {
    let aArray  = options.aArray;
    const id    = options.id;
    const val   = options.val; 
 
 
    if (aArray.length > 0) {
      let iLast = aArray.length-1;
      while(iLast >= 0 && aArray[iLast] == null) {
        iLast--;
      }
      // entire null list, reset it
      if (iLast < 0) {
        aArray = [{ id: id, val: val }];
        return;
      }

      const lastValue = aArray[iLast].val;
      if (val >= lastValue) {
        aArray.push({ id: id, val: val });
      }
      else {
        const t0 = Date.now();
        // todo as events are added during loadFromCache 
        // queue up any real time subscribe events task https://github.com/sp0n-7/pinballdb/issues/5

        // console.warn({ action: 'pinballdb._addToArray.out.of.order.err', size: aArray.length, id: id, val: val, lastValue: lastValue });
        // aArray.push({ id: id, val: val });

        // expensive insert internal to array O(N) copying index over
        // switch to ordered list if this is normal behavior
 
        // find insert position binary search of ordered array
        const index = binarySearch({
          aArray  : aArray,
          val     : val
        });
 
        // copy array over
        aArray.splice(index,0,{ id: id, val: val});
        // console.warn({ action: 'pinballdb._addToArray.out.of.order.err', size: aArray.length, id: id, val: val, lastValue: lastValue, spliceComplete: Date.now()-t0 });
      }
    }
    else {
      aArray.push({ id: id, val: val });
    }
  }

  _removeFromArray(options) {
    let aArray          = options.aArray;
    const id            = options.id;    
    const oOldDoc       = this.db[id];
    const sPropertyName = options.sPropertyName;
    const val           = oOldDoc[sPropertyName];
     
    if (oOldDoc) {
      // find matching val
      let index = findIndex({ aArray: aArray, id: id, val: val });
      if (index >= 0) {
        aArray[index] = null; // faster to set val to null than reallocate array with a splice
      }      
    }
  }  

  _addToData(options) {
    // array is append only, use cs instead of score
    this._addToArray( { aArray: this.aOrdered, id: options.id, val: options.cs     });
    
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
      this._removeFromArray({ aArray: this.aOrdered,  id: id, sPropertyName: 'cs' });
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
    if (typeof oDoc.score !== 'number' && typeof oDoc.ts === 'number') {
      oDoc.score = oDoc.ts || oDoc.cs;
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
  // pb:$cityCode:upsert and pb:$cityCode:remove channel messages
  // after subscription, keeps connection to cacheDB for future use
  addSubscriber(options) {
    const sAction   = Pinball.name + '.addSubscriber'; 
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
      setName       : 'pb',
      scoreProperty : 'cs'
    }

    this.cacheSubscriber = new CacheSubscriber(oSubscriberOptions)

    let   oProcs    = {};
    oProcs[`pb:${cityCode}:upsert`] = (oDoc) => {
      // console.log({ action: 'subscriber.proc.upsert', oDoc:oDoc });
      this.upsert(oDoc);
    }

    oProcs[`pb:${cityCode}:remove`] = (cacheId) => {
      const oDocId = this.cacheSubscriber.getDocId(cacheId);
      // console.log({ action: 'subscriber.proc.remove', cacheId: cacheId });
      if (oDocId != null && u.validString(oDocId.id)) {
        this.remove(oDocId.id);
      }
      else {
        console.error({ action: `oProcs.pb.${cityCode}.remove.err`, cacheId: cacheId })
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
    const sAction      = Pinball.name + '.loadFromCache';
    const sCacheUrl    = options.sCacheUrl;
    const aProps       = options.aProps;
    const cityCode     = this.cityCode;

    // pinball now has a rw cacheDB
    this.cacheDB       = new Cache({ sCacheUrl : sCacheUrl, setName: 'pb', scoreProperty: 'cs' });

    const t0 = Date.now();
    let t1;

    const scanPattern = `pb:${cityCode}:*`; // used for keys
    const setKey      = cache.getSortedSetName(cityCode);
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
      for (let i=0;i < aObjects.length;i++) {
        // console.log(aObjects[i].id,aObjects[i].cs)
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
    
    let N               = options.N;

    if (N <= 0) {
      return [];
    }

    const lowerLatitude     = options.lowerLatitude;
    const upperLatitude     = options.upperLatitude;
    const lowerLongitude    = options.lowerLongitude;
    const upperLongitude    = options.upperLongitude;
    const minIncidentLevel  = options.minIncidentLevel || 0;


    const matchingEvent = (oData) => {
      const level = typeof oData.level === 'number' ? oData.level : 1;
      return oData.latitude  > lowerLatitude  && oData.latitude  < upperLatitude  &&
             oData.longitude > lowerLongitude && oData.longitude < upperLongitude && 
             level >= minIncidentLevel;
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

        // this is how many events we will review for broad queries
        const NMaxMatched = 100
        let nearbyMinutesThreshold = 20 * 60 * 1000 //20 minutes

        // if there are more than N events in past 20min, return all
        const fCheckActive = (aArray,sProp) => {
          let nearbyMinutesThreshold = 20 * 60 * 1000 //20 minutes
          let nActive = aArray.filter( (a) => { return (Date.now() - a[sProp]) < nearbyMinutesThreshold }).length 
          // console.log({ action: 'fCheckActive', nActive: nActive })
          nActive = nActive < NMaxMatched ? nActive : NMaxMatched
          if (nActive > N) {
            return nActive
          }
          return N   
        }

        const fSortAscendingCheckActive = (aArray,sProp) => {
          const fSort = (a,b) => a[sProp] - b[sProp]  // array sorted in ascending order          
          if (aArray.length) {
            aArray.sort(fSort)
            const tNow = Date.now()
            let nActive = 0 // if most recent event older than tNow - nearbyMinutesThreshold, none are active
            if (aArray[aArray.length - 1][sProp] >= tNow - nearbyMinutesThreshold) {
              nActive = aArray.length - binarySearch({ aArray: aArray, val: tNow - nearbyMinutesThreshold })
              // nActive = aArray.filter( (a) => { return (Date.now() - a[sProp]) < nearbyMinutesThreshold }).length
            }

            // compare first and last to index found
            let index = Math.max(Math.min(nActive,aArray.length - 1),0)
            // console.log('binarySearch tNow-20mins',tNow-nearbyMinutesThreshold,'nActive',nActive,'aArray[index][sProp]',aArray[index][sProp],'first val',aArray[0][sProp],'last val',aArray[aArray.length - 1][sProp],'last val >= tNow - 20min',aArray[aArray.length - 1][sProp] >= tNow - nearbyMinutesThreshold)
            nActive = nActive < NMaxMatched ? nActive : NMaxMatched
            if (nActive > N) {
              return nActive
            }
          }
          return N   
        }


        const fSortDescendingCheckActive = (aArray,sProp) => {
          const fSort = (a,b) => b[sProp] - a[sProp] // array sorted in descending order          
          if (aArray.length) {
            aArray.sort(fSort)
            const tNow = Date.now()

            let nActive = 0 // if most recent event older than tNow - nearbyMinutesThreshold, none are active
            if (aArray[0][sProp] >= tNow - nearbyMinutesThreshold) {
              nActive = binarySearchDescending({ aArray: aArray, val: tNow - nearbyMinutesThreshold })  
              // nActive = aArray.filter( (a) => { return (Date.now() - a[sProp]) < nearbyMinutesThreshold }).length
            }

            // compare first and last to index found
            let index = Math.max(Math.min(nActive,aArray.length - 1),0)
            // console.log('binarySearchDescending tNow-20mins',tNow-nearbyMinutesThreshold,'nActive',nActive,'aArray[index][sProp]',aArray[index][sProp],'first val',aArray[0][sProp],'last val',aArray[aArray.length - 1][sProp],'first val >= tNow - 20min',aArray[0][sProp] >= tNow - nearbyMinutesThreshold)
            nActive = nActive < NMaxMatched ? nActive : NMaxMatched
            if (nActive > N) {
              return nActive
            }
          }
          return N   
        }

        let nActive = 0

        // brute force backwards score search 
        if (nTotal > this.NBucketThreshold) {

          const sortAndSlice = (aData) => {
            // let nActiveTest = aData.filter( (a) => { return (Date.now() - a.ts) < nearbyMinutesThreshold }).length

            // N = fCheckActive(aData,'ts')
            // return aData.sort( (a,b) => b.ts - a.ts).slice(0,N); // array sorted in descending order
            N = fSortDescendingCheckActive(aData,'ts')
            // console.log(`fSortAndCheckActive.array N:${N} nActiveTest:${nActiveTest}, aArrayLength: ${aData.length}, lastValue: ${aData[aData.length-1].ts}, nearbyMinutesThreshold: ${Date.now()-nearbyMinutesThreshold}`)
            return aData.slice(0,N); 
          }

          for (let i = this.aOrdered.length - 1;i > -1;i--) {
            const oMetaData = this.aOrdered[i];
            if (oMetaData) { // if non null
              const oData = this.db[oMetaData.id];
              if (matchingEvent(oData)) {
                // console.log('i',i,'oData.id',oData.id,'ts',oData.ts,'level',oData.level);
                aResults.push(oData);
                if (aResults.length == NMaxMatched) {
                  // console.timeEnd('totalFullScan');
                  return sortAndSlice(aResults);
                }
              }              
            }
          }
          // console.timeEnd('totalFullScan');          
          return sortAndSlice(aResults);
        }
        else {

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
          if( aArray.length !== 0) {
            // let nActiveTest = aArray.filter( (a) => { return (Date.now() - a.val) < nearbyMinutesThreshold }).length
            N = fSortAscendingCheckActive(aArray,'val')
            // console.log(`fSortAndCheckActive.grid N:${N} nActiveTest:${nActiveTest}, aArrayLength: ${aArray.length}, firstValue: ${aArray[0].val}, nearbyMinutesThreshold: ${Date.now()-nearbyMinutesThreshold}`)
            // N = fCheckActive(aArray,'val')
            // aArray.sort( (a,b) => a.val - b.val ); // array sorted in ascending order
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
          return aResults;
        }
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

module.exports = Pinball;
