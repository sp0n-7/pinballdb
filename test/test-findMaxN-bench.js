const findMaxN = require('../lib/util').findMaxN;

const aArraySet = [
];

const NArrays = 10000;
const fThres  = 0;
const NMaxDim = 500;
let   id      = 0;

const t0 = Date.now();
// load bench
for (let i=0;i < NArrays;i++) {
  const fRand = Math.random();

  if (fRand > fThres) {
    const NMembers = Math.random() * NMaxDim + 1;
    let aArray = [];
    for (let j=0;j < NMembers;j++) {
      aArray.push({ id: '-k'+id++, score: Date.now() + Math.random() })
    }
    aArraySet.push(aArray);
  }
  else {
    aArraySet.push(null);
  }

}


const t1 = Date.now();
console.log('loaded test data',t1-t0 + 'ms');

for (let i =0; i < 100;i++) {
  const t2 = Date.now();
  let aResults = findMaxN({ aArraySet: aArraySet, N: 20 });
  console.log(Date.now()-t2 + 'ms');
  // console.log('aResults',aResults,'time',Date.now()-t1 + 'ms');
}



// ok performance? 10k arrays, each of random length up to 501
// messel@messels-MBP:~/Desktop/Dropbox/code/js/db_tuts/pinball_tut/test$ node test-findMaxN-bench.js 
// loaded test data 1264ms
// aResults [ { id: '-k2531205', score: 1483110583799.8884 },
//   { id: '-k2531887', score: 1483110583799.8337 },
//   { id: '-k2531553', score: 1483110583799.222 },
//   { id: '-k2531552', score: 1483110583799.7883 },
//   { id: '-k2531204', score: 1483110583799.181 },
//   { id: '-k2531203', score: 1483110583799.484 },
//   { id: '-k2531202', score: 1483110583799.6582 },
//   { id: '-k2531201', score: 1483110583799.9253 },
//   { id: '-k2531200', score: 1483110583799.5496 },
//   { id: '-k2531199', score: 1483110583799.6528 },
//   { id: '-k2531198', score: 1483110583799.5276 },
//   { id: '-k2531197', score: 1483110583799.5483 },
//   { id: '-k2531196', score: 1483110583799.765 },
//   { id: '-k2531195', score: 1483110583799.3804 },
//   { id: '-k2531194', score: 1483110583799.5598 },
//   { id: '-k2531193', score: 1483110583799.4592 },
//   { id: '-k2531192', score: 1483110583799.5671 },
//   { id: '-k2531191', score: 1483110583799.1829 },
//   { id: '-k2531190', score: 1483110583799.048 },
//   { id: '-k2531189', score: 1483110583799.9211 } ] time 51ms