const binarySearch = require('../lib/util').binarySearch;

let anArray = [];

let N = 100;
for (let i = 0;i < N;i++) {
  let score = i;
  if (i % 2) {
    score = i - 1;
  }
  anArray.push({ id: '-k'+i, score: score})
}

let oItem = { id: '-k'+N, score: 50 };
const options0 = {
  aScores : anArray,
  score   : oItem.score
}

let index = binarySearch(options0);
anArray.splice(index,0, oItem)
console.log(index,anArray);