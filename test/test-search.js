const u            = require('../lib/util');
const binarySearch = u.binarySearch;
const findIndex    = u.findIndex;

let anArray = [];

let N = 1000000;
for (let i = 0;i < N;i++) {
  let val = i;
  if (i % 2) {
    val = i - 1;
  }
  if (Math.random() < 0.90) {
    anArray.push({ id: '-k'+i, val: val})
    // console.log('non null at',anArray[anArray.length-1])
  }
  else {
    // console.log('null at',i)
    anArray.push(null);
  }
}
// console.log(anArray)

for (let i = 0; i < N;i++) {
  let val = i;
  if (i % 2) {
    val = i - 1;
  }

  const options0 = {
    aArray  : anArray,
    val     : val
  }

  let index = binarySearch(options0);
  let i2 = findIndex({ aArray: anArray, id: '-k'+i, val: val})
  console.log(`index ${index} of val ${val} find index ${i2} of id ${'-k'+i} val ${val} is null? ${anArray[i]}`);  
}
