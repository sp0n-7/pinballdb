'use strict';

// const _ = require('lodash');

// busted internal insert, need to debug/patch

const binarySearch = (options) => {
  let aScores = options.aScores;
  let score   = options.score;

  // handle special cases
  if (aScores.length === 0 || score <= aScores[0]) { // empty scores, or before first score
    return 0;
  }
  else if (score >= aScores[aScores.length-1]) { // > after last score
    return aScores.length;
  }

  // split and check
  let iStart        = options.iStart || 0;
  let iEnd          = options.iEnd   || aScores.length;
  let index         = Math.floor((iEnd - iStart)/2) + iStart;
  // console.log({ aScores: aScores, indexMin1 : index -1, indexPlus1 : index +1, index: index })
  let fPrev         = index - 1 >= 0              ? aScores[index - 1].score : null;
  let fNext         = index + 1 < aScores.length  ? aScores[index + 1].score : null;
  let oComp         = aScores[index];
  let currentScore  = oComp.score;

  // length 1 array
  if (fPrev === null && fNext === null) {
    if (score < currentScore) {
      return index;
    }
    else {
      return index+1;
    }
  }
  else if (currentScore == score || ( (fPrev <= score || fPrev === null) && (score <= fNext || fNext === null) ) ) {
    let indexFound = index;
    for (let i = index;i < aScores.length;i++) {
      if (aScores[i] > score) {
        indexFound = i - 1;
        // console.log('index',index,'indexFound',indexFound);
      }
    }
    // found a location
    return indexFound;
  }
  else { // keep looking
    if (score < oComp.score) {
      return binarySearch({ aScores: aScores, score: score, iStart: iStart, iEnd: index});
    }
    else {
      return binarySearch({ aScores: aScores, score: score, iStart: index,  iEnd: iEnd});
    }
  }
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