'use strict';

const AVLTree         = require('binary-search-tree').AVLTree;
const binarySearch    = require('./util').binarySearch;
const findMaxNByScore = require('../lib/util').findMaxNByScore;

const sClass = 'Pinball';
class Pinball {
  constructor(options) {
    this.db       = {}; // kv in memory store

    this.scale    = 1e6;

    // avltree helper funcs
    // function compareKeys (a, b) {
    //   if (a.val < b.val) { return -1; }
    //   if (a.val > b.val) { return 1; }
    //   return 0;
    // }

    function checkValueEquality (a, b) {
      return a.id === b.id;
    }    

    // global ordered by score array
    this.aOrdered = [];
    // this.tLats    = new AVLTree({ compareKeys: compareKeys, checkValueEquality: checkValueEquality });
    // this.tLons    = new AVLTree({ compareKeys: compareKeys, checkValueEquality: checkValueEquality });
    this.tLats    = new AVLTree({ checkValueEquality: checkValueEquality });
    this.tLons    = new AVLTree({ checkValueEquality: checkValueEquality });

    // total region covered lat & lon
    this.lowerLatitude  = options.lowerLatitude;
    this.upperLatitude  = options.upperLatitude;
    this.lowerLongitude = options.lowerLongitude;
    this.upperLongitude = options.upperLongitude;
  }

    // options: { aArray: aArray, id:id, val: val }
  _addToArray(options) {
    let aArray  = options.aArray;
    const id    = options.id;
    const val   = options.val; 
 
 
    if (aArray.length) {
      const lastValue = aArray[aArray.length - 1];
      if (val >= lastValue) {
        aArray.push({ id: id, val: val });
      }
      else {
        // expensive insert internal to array O(N) copying index over
 
        // find insert position binary search of ordered array
        const index = binarySearch({
          aArray  : aArray,
          val     : val
        });
 
        // copy array over
        aArray.splice(index,0,{ id: id, val: val});
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
        while (index < aArray.length && aArray[index+1].val === oOldDoc[sPropertyName]) {
          if (id === aArray[index].id) {
            return index;
          }      
          index++;
        } 
        return indexFound; // no match of id in array   
      }
 
      let index = findMatch(indexStart);
      if (index >= 0) {
        aArray.splice(index,1); // slow compared to internal remove from dual linked list removal etc      
      }      
    }
  }  

  // options: { id:id, val: val }
  _addToTree(options) {
    let tTree = options.tTree;
    const id  = options.id;
    const val = options.val;
    const key = Math.floor(val * this.scale);

    // tTree.insert({ val: val}, { id: id, val: val});
    tTree.insert(key, { id: id, val: val});
  }

  _removeFromTree(options) {
    let tTree           = options.tTree;
    const id            = options.id;    
    const oOldDoc       = this.db[id];
    const sPropertyName = options.sPropertyName;
    const val           = oOldDoc[sPropertyName];
    const key           = Math.floor(val * this.scale);
    
    if (oOldDoc) {
      tTree.delete(key, { id: id, val: val });
    }
  }

  _addToData(options) {
    this._addToArray({ aArray: this.aOrdered, id: options.id, val: options.score     });
    this._addToTree( { tTree:  this.tLats,    id: options.id, val: options.latitude  });
    this._addToTree( { tTree:  this.tLons,    id: options.id, val: options.longitude });
  }


  _removeFromData(id) {
    const oOldDoc = this.db[id];
    if (oOldDoc) {
      this._removeFromArray({ aArray: this.aOrdered,  id: id, sPropertyName: 'score'     })
      this._removeFromTree( { tTree:  this.tLats,     id: id, sPropertyName: 'latitude'  })
      this._removeFromTree( { tTree:  this.tLons,     id: id, sPropertyName: 'longitude' })
    }
  }

  // oDoc's key is oDoc.id
  upsert(oDoc) {
    // if valid
    if (typeof oDoc.longitude === 'number' && typeof oDoc.latitude === 'number') {
      // if object id is already in db update
      if (this.db[oDoc.id]) {
        this._removeFromTrees(id);
      }
      this.db[oDoc.id] = oDoc;
      this._addToData({ id: oDoc.id, score: oDoc.score, latitude: oDoc.latitude, longitude: oDoc.longitude });
    }

  }

  batchUpsert(batch) {
    for (oDoc of batch) {
      this.upsert(oDoc)
    }
  }


  remove(id) {
     if (this.db[oDoc.id]) {
      this._removeFromTrees(oDoc.id);
      this.db[oDoc.id] = null;
     }
  }

  query(options) {
    let aResults  = [];
    
    const lowerLatitude   = options.lowerLatitude;
    const upperLatitude   = options.upperLatitude;
    const lowerLongitude  = options.lowerLongitude;
    const upperLongitude  = options.upperLongitude;

    const kLowerLatitude  = Math.floor(lowerLatitude  * this.scale);
    const kUpperLatitude  = Math.floor(upperLatitude  * this.scale);
    const kLowerLongitude = Math.floor(lowerLongitude * this.scale);
    const kUpperLongitude = Math.floor(upperLongitude * this.scale);


    const N               = options.N;

    const t0 = Date.now();
    
    // if query overlaps with db
    const bOutsideBounds  = this.lowerLatitude  > upperLatitude  || this.upperLatitude < lowerLatitude || 
                            this.lowerLongitude > upperLongitude || this.upperLongitude < lowerLongitude;

    if (!bOutsideBounds) {

      const t1 = Date.now();
      // utilize ordered aLatitudes, aLongitudes to determine how many events are within the region
      const fAreaRegion = (upperLatitude - lowerLatitude) * (upperLongitude - lowerLongitude);


      if (fAreaRegion < 0.0000015) {
        // let aLats = this.tLats.betweenBounds({ $lt: { val: upperLatitude }, $gte: { val: lowerLatitude } }) || [];       
        // let aLons = this.tLons.betweenBounds({ $lt: { val: upperLongitude}, $gte: { val: lowerLongitude} }) || [];       
        let aLats = this.tLats.betweenBounds({ $lt: kUpperLatitude,  $gte: kLowerLatitude  }) || [];       
        let aLons = this.tLons.betweenBounds({ $lt: kUpperLongitude, $gte: kLowerLongitude }) || [];       

        console.log('aLatsDim',aLats.length,'aLonsDim',aLons.length,'fAreaRegion',fAreaRegion,'avl tree scan',Date.now()-t1);        
      }

      // something smarter with small matching number < N? of not many latitudes and longitudes
      // possible: intersection of matching latitude ids and longitude ids

      // brute force backwards score search      
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
