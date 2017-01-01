const binarySearch = require('../lib/util').binarySearch;

let anArray = [
  { id: 'a', score: 0    },
  { id: 'b', score: 3    },
  { id: 'c', score: 5.4  },
  { id: 'd', score: 5.4  },
  { id: 'e', score: 13   } 
];

let oItem = { id: 'f', score: 5.4 };
const options0 = {
  aScores : anArray,
  score   : oItem.score
}

let index = binarySearch(options0);
anArray.splice(index,0, oItem)
console.log(index,anArray);