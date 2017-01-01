'use strict';

// const _ = require('lodash');

// modified from http://oli.me.uk/2013/06/08/searching-javascript-arrays-with-a-binary-search/
const binarySearch = (options) => {
    let aScores = options.aScores;
    let score   = options.score;

    // searchElement 
    let minIndex = 0;
    let maxIndex = aScores.length - 1;
 
    while (minIndex <= maxIndex) {
      const currentIndex = Math.floor( (minIndex + maxIndex) / 2);
      const currentScore = aScores[currentIndex].score;

      if (currentScore < score) {
        minIndex = currentIndex + 1;
      }
      else if (currentScore > score) {
        maxIndex = currentIndex - 1;
      }
      else {
        return currentIndex;
      }
    }
 
    return aScores.length;
}


// find N max from M ordered arrays, could extend to full merge of sorted arrays but not needed
const findMaxNByScore = (options) => {
  // const t0 = process.hrtime();

  const aArraySet   = options.aArraySet;
  const N           = options.N;
  let iFound        = 0;
  let nElementsLeft = 0;
  let aResults      = [];
  let aMax          = []; // heap of all ordered array maximums, used to keep track of last checked index per array
  let aScores       = []; // pure array of scores for faster max finding
  // let aScores       = new Float64Array(aArraySet.length); // pure array of scores for faster max finding
  aMax.length       = aArraySet.length;
  aScores.length    = aArraySet.length

  // initialize aMax heap
  for (let i = 0; i < aArraySet.length;i++) {
    const aArray = aArraySet[i];
    if (aArray && aArray.length > 0) {
      const iMaxArrayIndex = aArray.length - 1; // used to grab next maximum
      aMax[i] = { iMaxArrayIndex: iMaxArrayIndex, oData : aArray[iMaxArrayIndex] };
      aScores[i] = aArray[iMaxArrayIndex].score;
      // const oData = aArray[iMaxArrayIndex];
      // aMax[i] = { iMaxArrayIndex: iMaxArrayIndex, oData : oData };
      // aScores[i] = oData.score;
      nElementsLeft += iMaxArrayIndex + 1;
    }
  }
  // const d0 = process.hrtime(t0)[1] / 1000000;

  // let d1 = 0, d2 = 0;

  while (iFound < N && nElementsLeft > 0) {
    // const t1 = process.hrtime();
    
    let iMaxIndex = null;
    let dMaxScore = -1e9; 

    for (let i = 0;i < aScores.length;i++) {
      // if (aMax[i] && aMax[i].oData.score > dMaxScore) {
      if (aScores[i] > dMaxScore) {
        iMaxIndex = i;
        dMaxScore = aScores[i];
      }
    }

    // d1 += process.hrtime(t1)[1] / 1000000;    
    // const t2 = process.hrtime();

    // if we found something 
    if (iMaxIndex != null) {
      const oMax = aMax[iMaxIndex];
      aResults.push(oMax.oData);        //   add to results
      iFound++;                         //   increment iFound
      if (iFound == N) {
        // d2 += process.hrtime(t2)[1] / 1000000;
        // console.log('d0',d0,'d1',d1,'d2',d2,'sum',[d0,d1,d2].reduce( (a, b) => { return a + b; }, 0),'total',process.hrtime(t0)[1] / 1000000);
        return aResults;                //  immediate return
      }
      nElementsLeft--;                  //   decrement nElementsLeft

      //   pop next max from end of max array in aMax heap
      if (oMax.iMaxArrayIndex > 0) {
        const iNext         = oMax.iMaxArrayIndex - 1;
        aMax[iMaxIndex]     = { iMaxArrayIndex: iNext, oData: aArraySet[iMaxIndex][iNext] };
        aScores[iMaxIndex]  = aMax[iMaxIndex].oData.score;
      }
      else {
        aMax[iMaxIndex]     = null
        aScores[iMaxIndex]  = null;
      }
    }
    else {
      // no data break
      break;
    }
    // d2 += process.hrtime(t2)[1] / 1000000;
  }
  // console.log('d0',d0,'d1',d1,'d2',d2,'sum',[d0,d1,d2].reduce( (a, b) => { return a + b; }, 0),'total',process.hrtime(t0)[1] / 1000000);

  return aResults;
}



module.exports = {
  binarySearch        : binarySearch,
  findMaxNByScore     : findMaxNByScore
}