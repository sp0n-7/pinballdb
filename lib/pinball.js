'use strict';

const binarySearch      = require('./util').binarySearch;
const cache             = require('./cache');
const subscribeToCache  = cache.subscribeToCache;
const CacheWriter       = cache.CacheWriter;
const Promise           = require('bluebird'); // used for .map with concurrency, batch promises

const sClass = 'Pinball';
class Pinball {
  constructor(options) {
    this.db       = {}; // kv in memory store

    // an object which includes which bucket each object falls within ie this.meta[id].aBucketIndex
    this.meta     = {}; 

    // global ordered by score array
    this.aOrdered = [];

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

    if (typeof options.cacheUrl === 'string' && options.cacheUrl.length > 0) {
      this.cacheWriter = new CacheWriter({ sCacheUrl : options.cacheUrl });
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
    const sAction = sClass + '._addToBucket';
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

      this.grid[i][j].aArray.push({ id: id, latitude: options.latitude, longitude: options.longitude, val: options.val });
      this.grid[i][j].nBucketTotal++;

      // update meta
      this.meta[id] = { aBucketIndex : [i,j] };
    }
    else {
      this.offGridBucket.aArray.push({ id: id, latitude: options.latitude, longitude: options.longitude, val: options.val });
      this.offGridBucket.nBucketTotal++;

      // update meta
      this.meta[id] = { bOffGrid : true };

    }
  }  

  // deletions are slow, needs to scan through grid aArrays for id
  _removeFromBucket(id) {
    const sAction = sClass + '._removeFromBucket';
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
 
 
    if (aArray.length) {
      const lastValue = aArray[aArray.length - 1].val;
      if (val >= lastValue) {
        aArray.push({ id: id, val: val });
      }
      else {
        // let them be out of order until you swap to a list
        console.error({ action: 'pinballdb._addToArray.out.of.order.err', val: val, lastValue: lastValue });

        // // expensive insert internal to array O(N) copying index over
 
        // // find insert position binary search of ordered array
        // const index = binarySearch({
        //   aArray  : aArray,
        //   val     : val
        // });
 
        // // copy array over
        // aArray.splice(index,0,{ id: id, val: val});
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
      let indexStart = binarySearch({
        aArray  : aArray,
        val     : val
      });
 
      // values could be duplicates so find first and check all identical
      // values for the id we want to remove
      const findMatch = (initialIndex) => {
        let indexFound = -1;
 
        let index = initialIndex;
 
        // set index to first identical score
        while (index > 0 && aArray[index-1].val === oOldDoc[sPropertyName]) {
          index--;
        }
 
        // scan through all identical scores until id is found
        while (index < aArray.length - 1 && aArray[index+1].val === oOldDoc[sPropertyName]) {
          if (id === aArray[index].id) {
            return index;
          }      
          index++;
        } 
        return indexFound; // no match of id in array   
      }
 
      let index = findMatch(indexStart);
      if (index >= 0) {
        // aArray.splice(index,1); // slow compared to internal remove from linked list removal etc      
        aArray[index] = null; // faster to set val to null than reallocate array with a splice
      }      
    }
  }  

  _addToData(options) {
    this._addToArray( { aArray: this.aOrdered, id: options.id, val: options.score     });
    
    this._addToBucket({ 
      id: options.id, 
      longitude: options.longitude, 
      latitude: options.latitude, 
      val: options.score 
    });
  }


  _removeFromData(id) {
    const oOldDoc = this.db[id];
    if (oOldDoc) {
      this._removeFromArray({ aArray: this.aOrdered,  id: id, sPropertyName: 'score'     })
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
  _upsert(oDoc) {
    // if valid
    if (typeof oDoc.longitude === 'number' && typeof oDoc.latitude === 'number') {
      // if object id is already in db update
      if (this.db[oDoc.id]) {
        this._removeFromData(id);
      }
      this.db[oDoc.id] = oDoc;
      this._addToData({ id: oDoc.id, score: oDoc.score, latitude: oDoc.latitude, longitude: oDoc.longitude });
    }
  }

  _batchUpsert(batch) {
    for (oDoc of batch) {
      this.upsert(oDoc)
    }
  }


  _remove(id) {
     if (this.db[id]) {
      this._removeFromData(id);
      this.db[id] = null;
     }
  }

  upsertCache(oDoc) {
    if (this.cacheWriter) {
      return this.cacheWriter.upsert(oDoc);
    }
    else {
      return Promise.reject(Error('pinballdb.upsertCache.err no cacheWriter set'))
    }
  }

  batchUpsertCache(aDocArray) {
    // return Promise.resolve(aDocArray).map(this.upsertCache.bind(this),{ concurrency: 1 });
    return Promise.each(aDocArray, this.upsertCache.bind(this));
  }

  removeFromCache(id) {
    if (this.cacheWriter) {
      return this.cacheWriter.remove(id);
    }
    else {
      return Promise.reject(Error('pinballdb.removeFromCache.err no cacheWriter set'))
    }
  }

  batchRemoveFromCache(aIdArray) {
    // return Promise.resolve(aIdArray).map(this.removeFromCache.bind(this),{ concurrency: 1 });
    return Promise.each(aIdArray, this.removeFromCache.bind(this));
  }


  addSubscriber(sCacheUrl) {
    const oSubscriberOptions =  { 
      sCacheUrl : sCacheUrl,
      oProcs    : {
        upsert: (oDoc) => {
          // console.log({ action: sClass + '.addSubscriber.proc.upsert', oDoc:oDoc });
          this._upsert(oDoc);
        },
        remove: (id) => {
          // console.log({ action: sClass + '.addSubscriber.proc.remove', id: id });
          this._remove(id);
        }
      }
    }
    return subscribeToCache(oSubscriberOptions).then( sub => {
      this.cacheSubscriber = sub; 
      return;
    })
  }

  query(options) {

    let aResults  = [];
    
    const N               = options.N;

    if (N <= 0) {
      return [];
    }

    const lowerLatitude   = options.lowerLatitude;
    const upperLatitude   = options.upperLatitude;
    const lowerLongitude  = options.lowerLongitude;
    const upperLongitude  = options.upperLongitude;


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


        if (nTotal > this.NBucketThreshold) {
          // brute force backwards score search      
          for (let i = this.aOrdered.length - 1;i > -1;i--) {
            const oData = this.db[this.aOrdered[i].id];
            if (oData.latitude  > lowerLatitude  && oData.latitude  < upperLatitude &&
                oData.longitude > lowerLongitude && oData.longitude < upperLongitude) {
              // console.log('i',i,'oData.id',oData.id,'ts',oData.ts)
              aResults.push(oData);
              if (aResults.length == N) {
                // console.timeEnd('totalFullScan');
                return aResults;
              }
            }
          }
          // console.timeEnd('totalFullScan');          
          return aResults;
        }
        else {

          // console.time('loadAndSortArray');
          let aArray = [];
          for (let i = 0; i < aBuckets.length;i++) {
            let aDataSet = aBuckets[i];
            for (let j = 0;j < aDataSet.length;j++) {
              const oData = aDataSet[j];
              // skip null members
              if (oData) {
                if (oData.latitude  > lowerLatitude  && oData.latitude  < upperLatitude &&
                    oData.longitude > lowerLongitude && oData.longitude < upperLongitude) 
                {
                  aArray.push(oData);
                }
              }
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
      }
    }
    else {
      // check off grid
      let aArray = [];
      const aDataSet = this.offGridBucket.aArray;
      for (let j = 0;j < aDataSet.length;j++) {
        const oData = aDataSet[j];
        // skip null members
        if (oData) {
          if (oData.latitude  > lowerLatitude  && oData.latitude  < upperLatitude &&
              oData.longitude > lowerLongitude && oData.longitude < upperLongitude) 
          {
            aArray.push(oData);
          }
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
