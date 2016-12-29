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

module.exports = {
  binarySearch: binarySearch
}