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
    while (aArray[currentIndex] == null && currentIndex < maxIndex) {
      // console.log('skipping a null at',currentIndex)
      currentIndex++;
    }

    if (aArray[currentIndex] == null) {
      // console.log('still null at',currentIndex);
      maxIndex = Math.floor( (minIndex + maxIndex) / 2) - 1;
    }
    else {
      // console.log('found not null at',currentIndex,'val',aArray[currentIndex]);
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
  }

  // had to nudge the final returned index up by one if min and max met 1 before
  if (currentIndex >= 0 && aArray[currentIndex] && aArray[currentIndex].val < val) {
    currentIndex++;
  }

  return currentIndex;
}

const findIndex = (options) => {
  const aArray = options.aArray;
  const id     = options.id;
  const val    = options.val;

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
    while (index > 0 && (aArray[index-1] == null || aArray[index-1].val === val) ) {
      index--;
    }

    // scan through all identical scores until id is found
    while (index < aArray.length && (aArray[index] == null || aArray[index].val === val) ) {
      if (aArray[index] && id === aArray[index].id) {
        return index;
      }      
      index++;
    } 
    return indexFound; // no match of id in array   
  }

  return findMatch(indexStart);
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
  mergeSortedArrays   : mergeSortedArrays,
  findIndex           : findIndex
}