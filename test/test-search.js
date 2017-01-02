const binarySearch = require('../lib/util').binarySearch;

let anArray = [];

let N = 20;
for (let i = 0;i < N;i++) {
  let val = i;
  if (i % 2) {
    val = i - 1;
  }
  anArray.push({ id: '-k'+i, val: val})
}

for (let i = N; i < 2*N;i++) {
  let oItem = { id: '-k'+i, val: i - N };
  const options0 = {
    aArray  : anArray,
    val     : oItem.val
  }

  let index = binarySearch(options0);
  anArray.splice(index,0, oItem)
  console.log(index,anArray);  
}
