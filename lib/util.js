'use strict';

// const _ = require('lodash');

// modified from http://oli.me.uk/2013/06/08/searching-javascript-arrays-with-a-binary-search/
// finds an insertion point for the given value
const binarySearch = (options) => {
    let aArray  = options.aArray;
    let val     = options.val;


    // searchElement 
    let currentIndex = -1;
    let minIndex = 0;
    let maxIndex = aArray.length - 1;
 
    while (minIndex <= maxIndex) {
      currentIndex = Math.floor( (minIndex + maxIndex) / 2);
      const currentVal = aArray[currentIndex].val;

      if (currentVal < val) {
        minIndex = currentIndex + 1;
      }
      else if (currentVal > val) {
        maxIndex = currentIndex - 1;
      }
      else {
        return currentIndex;
      }
    }

    // had to nudge the final returned index up by one if min and max met 1 before
    if (currentIndex >= 0 && aArray[currentIndex].val < val) {
      currentIndex++;
    }
 
    return currentIndex;
}

const mergeSortedArrays = (a,b,sProperty) => {
  let aMerged = [];
  let i = 0, j = 0, k = 0;

  while (i < a.length && j < b.length) {
    aMerged[k++] = a[i][sProperty] < b[j][sProperty] ? a[i++] :  b[j++];
  }

  while (i < a.length)  
    aMerged[k++] = a[i++];

  while (j < b.length) {
    aMerged[k++] = b[j++];    
  }    

  return aMerged;
}

module.exports = {
  binarySearch        : binarySearch,
  mergeSortedArrays   : mergeSortedArrays
}