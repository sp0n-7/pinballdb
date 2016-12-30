'use strict';

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
    if (score <= currentScore) {
      return index;
    }
    else {
      return index+1;
    }
  }
  else if (currentScore == score || ( (fPrev <= score || fPrev === null) && (score <= fNext || fNext === null) ) ) {
    // found a location
    return index;
  }
  else { // keep looking
    if (score < oComp.score) {
      return binarySearch({ aScores: aScores, id: id, score: score, iStart: iStart, iEnd: index});
    }
    else {
      return binarySearch({ aScores: aScores, id: id, score: score, iStart: index,  iEnd: iEnd});
    }
  }
}

// find N max from M ordered arrays, could extend to full merge of sorted arrays but not needed
const findMaxN = (options) => {
  const aArraySet   = options.aArraySet;
  const N           = options.N;
  let iFound        = 0;
  let nElementsLeft = 0;
  let aResults      = [];
  let aMax          = []; // heap of all ordered array maximums
  let aScores       = []; // pure array of scores for faster max finding
  aMax.length       = aArraySet.length;
  aScores.length    = aArraySet.length;

  const t0 = process.hrtime();
  // initialize aMax heap
  for (let i = 0; i < aArraySet.length;i++) {
    const aArray = aArraySet[i];
    if (aArray && aArray.length > 0) {
      const iMaxArrayIndex = aArray.length - 1; // used to grab next maximum
      aMax[i] = { iMaxArrayIndex: iMaxArrayIndex, oData : aArray[iMaxArrayIndex] };
      aScores[i] = aMax[i].oData.score;
      nElementsLeft += iMaxArrayIndex + 1;
    }
  }
  const d0 = process.hrtime(t0)[1] / 1000000;
  console.log('init',d0);
  let d1 = 0, d2 = 0;

  while (iFound < N && nElementsLeft > 0) {

    let iMaxIndex = null;
    let oMax      = null;
    let dMaxScore = -1e9; 

    const t1 = process.hrtime();
    // scan all array ends and find max
    for (let i = 0;i < aMax.length;i++) {
      if (aScores[i] > dMaxScore) {
        iMaxIndex = i;
        oMax      = aMax[i];
        dMaxScore = aScores[i];
      }
    }
    
    d1 += process.hrtime(t1)[1] / 1000000;    
    const t2 = process.hrtime();

    // if we found something 
    if (iMaxIndex != null && oMax) {
      aResults.push(oMax.oData);        //   add to results
      iFound++;                         //   increment iFound
      if (iFound == N) {
        d2 += process.hrtime(t2)[1] / 1000000;
        console.log('d1',d1,'d2',d2);
        return aResults;                //  immediate return
      }
      nElementsLeft--;                  //   decrement nElementsLeft

      //   pop next max from end of max array in aMax heap
      if (oMax.iMaxArrayIndex > 0) {
        const iNext     = oMax.iMaxArrayIndex - 1;
        aMax[iMaxIndex] = { iMaxArrayIndex: iNext, oData: aArraySet[iMaxIndex][iNext] };
        aScores[iMaxIndex] = aMax[iMaxIndex].oData.score;
      }
    }
    else {
      // no data break
      break;
    }
    d2 += process.hrtime(t2)[1] / 1000000;
  }
  console.log('d1',d1,'d2',d2);

  return aResults;
}

module.exports = {
  binarySearch        : binarySearch,
  findMaxN            : findMaxN
}