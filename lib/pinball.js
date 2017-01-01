'use strict';

const binarySearch    = require('./util').binarySearch;
const findMaxNByScore = require('../lib/util').findMaxNByScore;

const sClass = 'Pinball';
class Pinball {
  constructor(options) {
    this.db       = {}; // kv in memory store

    // an object which includes which bucket each object falls within ie this.meta[id].aBucketIndexes
    this.meta     = {}; 

    // global ordered by score array
    this.aOrdered = [];

    // total space lat by lon
    this.lowerLatitude  = options.lowerLatitude;
    this.upperLatitude  = options.upperLatitude;
    this.lowerLongitude = options.lowerLongitude;
    this.upperLongitude = options.upperLongitude;

    // grid resolution lat, lon: grid footprint not constant area
    this.deltaLatitude  = options.deltaLatitude;
    this.deltaLongitude = options.deltaLongitude;

    // grid dimensions
    this.NLatitude      = Math.ceil( (this.upperLatitude  - this.lowerLatitude ) / this.deltaLatitude  );
    this.NLongitude     = Math.ceil( (this.upperLongitude - this.lowerLongitude) / this.deltaLongitude );

    // grid, NLongitude by NLatitude buckets
    // each bucket is an object with an array aOrder, and oIndex id to aOrder index mapping
    // use assumption:
    //   inserts are common with increasing score (order)
    //   insertions with with lower scores, and deletions are rare
    this.grid = []
    this.grid.length = this.NLongitude;
    for (let i = 0; i < this.NLongitude;i++) {
      this.grid[i] = [];
      this.grid[i].length = this.NLatitude;
      for (let j = 0; j < this.NLatitude;j++) {
        // a bucket
        // looked briefly at binary search trees and avl trees https://en.wikipedia.org/wiki/AVL_tree
        // if we need faster insertions
        this.grid[i][j] = {
          aOrder  : [],
          oIndex  : {}
        }
      }
    }
  }

  // internal methods noted with leading underscore
  
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
    // console.log('longitude',options.longitude,'lower grid lon',this.lowerLongitude,'delta lon',this.deltaLongitude,'iLongitude',iLongitude)
    if (iLongitude < 0) iLongitude = 0;
    else if (iLongitude >= this.NLongitude - 1 ) iLongitude = this.NLongitude - 1;

    let iLatitude  = Math.floor( (options.latitude  - this.lowerLatitude ) / this.deltaLatitude );
    // console.log('latitude',options.latitude,'lower grid lat',this.lowerLatitude,'delta lat',this.deltaLatitude,'iLatitude',iLatitude)
    if (iLatitude < 0) iLatitude = 0;
    else if (iLatitude >= this.NLatitude - 1 ) iLatitude = this.NLatitude - 1;

    return [iLongitude,iLatitude];
  }


  _removeFromBucket(id) {
    const aStoredBI = this.meta[id].aBucketIndexes;    
    if (aStoredBI) {
      const i = aStoredBI[0];
      const j = aStoredBI[1];
      let oBucket = this.grid[i][j];
      const iPosition = oBucket.oIndex[id];
      // prefer to grow memory vs cleanup
      // allow null values in aOrder (maintains all relative oIndex)
      // can generate a non null output array as needed for queries
      // allow garbage collector to handle the null oIndex
      if (iPosition) {
        oBucket.aOrder[iPosition] = null;
        oBucket.oIndex[options.id] = null;        
      }
      // finally remove meta data for that doc
      this.meta[id] = null;
    }
  }

  // options: { id:id, longitude: longitude, latitude: latitude, score: score }
  _addToBucket(options) {
    const aBI   = this._getBucketIndexes(options);
    const id    = options.id;
    const score = options.score; 


    if (this.aOrdered.length) {
      const lastScore = this.aOrdered[this.aOrdered.length - 1];
      if (score >= lastScore) {
      // if (score > lastScore) {
        this.aOrdered.push({ id: id, score: score });
      }
      else {
        // expensive insert internal to array O(N) copying index over

        // find insert position binary search of ordered array
        const index = binarySearch({
          aScores : this.aOrdered,
          score   : score
        });
        // console.log('score',score,'index',index,'this.aOrdered',this.aOrdered)

        // copy array over
        this.aOrdered.splice(index,0,{ id: id, score: score});
        // console.log('spliced aOrdered',this.aOrdered)
      }
    }
    else {
      this.aOrdered.push({ id: id, score: score });
    }

    if (aBI) {

      const i = aBI[0];
      const j = aBI[1];
      // console.log('i',i,'j',j,'grid length',this.grid.length,'grid i length',this.grid[i].length)
      let oBucket = this.grid[i][j];
      let aOrder  = oBucket.aOrder;
      let oIndex  = oBucket.oIndex;

      if (aOrder.length) {
        const lastScore = aOrder[aOrder.length - 1];
        if (score >= lastScore) {
          aOrder.push({ id: id, score: score });
          oIndex[id] = aOrder.length - 1;
        }
        else {
          // expensive insert internal to array O(N) copying index over

          // find insert position binary search of ordered array
          const index = binarySearch({
            aScores : aOrder,
            score   : score
          });

          // copy array over and recreate oIndex
          aOrder.splice(index,0,{ id: id, score: score});
          let oNewIndex = {};
          for (let i=0;i < aOrder.length;i++) {
            oNewIndex[aOrder[i].id] = i;
          }
          this.grid[i][j].oIndex = oNewIndex;
        }
      }
      else {
        aOrder[0] = { id: id, score: score };
        oIndex[id] = 0;
      }

      // update meta
      this.meta[id] = { aBucketIndex : [i,j] }
    }    
  }


  // oDoc's key is oDoc.id
  upsert(oDoc) {
    // if valid
    if (typeof oDoc.longitude === 'number' && typeof oDoc.latitude === 'number') {
      // if object id is already in grid in a different bucket, need to remove it and re-add
      if (this.db[oDoc.id]) {
        const aBucketIndex = this._getBucketIndexes({ longitude: oDoc.longitude, latitude: oDoc.latitude });
        const aStoredBI    = this.meta[oDoc.id].aBucketIndex;
        if (aStoredBI && ( (aStoredBI[0] != aBucketIndex[0]) || (aStoredBI[1] != aBucketIndex[1]) ) ) {
          this._removeFromBucket(oDoc.id);
        }
      }
      this.db[oDoc.id] = oDoc;
      this._addToBucket({ id: oDoc.id, longitude: oDoc.longitude, latitude: oDoc.latitude, score: oDoc.score });
    }

  }

  batchUpsert(batch) {
    for (oDoc of batch) {
      this.upsert(oDoc)
    }
  }


  remove(id) {
     if (this.db[oDoc.id]) {
      this._removeFromBucket(oDoc.id);
      this.db[oDoc.id] = null;
     }
  }

  query(options) {
    // lowerLongitude,lowerLatitude,upperLongitude,upperLatitude,N
    // return last N from all buckets within search window
    const aLLIndexes = this._getLimitedBucketIndexes({ longitude: options.lowerLongitude, latitude: options.lowerLatitude });
    const aURIndexes = this._getLimitedBucketIndexes({ longitude: options.upperLongitude, latitude: options.upperLatitude });

    let aResults  = [];
    let aArraySet = [];
    // ilongitude loop
    if (aLLIndexes && aURIndexes) {
      for (let i = aLLIndexes[0];i < aURIndexes[0];i++) {
        // ilatitude loop
        for (let j = aLLIndexes[1];j < aURIndexes[1];j++) {
          aArraySet.push(this.grid[i][j].aOrder)
        }
      }
      // console.log('aArraySet',aArraySet);

      aResults = findMaxNByScore({ aArraySet: aArraySet, N: options.N }); 
      // aResults = findMaxN({ aArraySet: aArraySet, N: options.N }); 
    }
    return aResults;
  }


  queryBruteForce(options) {
    let aResults  = [];
    
    const lowerLatitude   = options.lowerLatitude;
    const upperLatitude   = options.upperLatitude;
    const lowerLongitude  = options.lowerLongitude;
    const upperLongitude  = options.upperLongitude;
    const N               = options.N;
    
    // if query overlaps with db
    const aLLIndexes      = this._getLimitedBucketIndexes({ longitude: lowerLongitude, latitude: lowerLatitude });
    const aURIndexes      = this._getLimitedBucketIndexes({ longitude: upperLongitude, latitude: upperLatitude });
    if (aLLIndexes && aURIndexes) {
      for (let i = this.aOrdered.length - 1;i > 0;i--) {
        const oData = this.db[this.aOrdered[i].id];
        if (oData.latitude  > lowerLatitude  && oData.latitude  < upperLatitude &&
            oData.longitude > lowerLongitude && oData.longitude < upperLongitude) {
          // console.log('i',i,'oData.id',oData.id,'ts',oData.ts)
          aResults.push(oData);
          if (aResults.length == N) {
            return aResults;
          }
        }
      }
    }
    return aResults;
  }

}

module.exports = Pinball;
