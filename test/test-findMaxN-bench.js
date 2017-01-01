const findMaxNByScore = require('../lib/util').findMaxNByScore;

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
    const tStart = Date.now();
    for (let j=0;j < NMembers;j++) {
      const oDoc = { id: '-k'+id++, score: tStart + j };
      aArray.push(oDoc);
    }
    aArraySet.push(aArray);
  }
  else {
    aArraySet.push(null);
  }

}


const t1 = Date.now();
console.log('loaded test data',t1-t0 + 'ms');

for (let i =0; i < 1;i++) {
  const t2 = Date.now();
  let aRandomSubset = [];
  for (let j=0;j < NArrays;j++) {
    const fRand = Math.random();
    if (fRand > -1) {
      aRandomSubset.push(aArraySet[j]);
    }
  }
  const t3 = Date.now();
  let aResults = findMaxNByScore({ aArraySet: aRandomSubset, N: 20 });
  const t4 = Date.now();
  // console.log(Date.now()-t2);
  console.log('random sampled time',t3-t2,'time',Date.now()-t3);
  // console.log('aResults',aResults);
}



// ok performance? 10k arrays, each of random length up to 501
// loaded test data 980ms
// init 4.210931
// d1 5.122258000000001 d2 0.119447
// time 48ms
// aResults [ { id: '-k2491446', score: 1483130099998 },
//   { id: '-k2487098', score: 1483130099997 },
//   { id: '-k2491445', score: 1483130099997 },
//   { id: '-k2487097', score: 1483130099996 },
//   { id: '-k2491444', score: 1483130099996 },
//   { id: '-k2487096', score: 1483130099995 },
//   { id: '-k2491443', score: 1483130099995 },
//   { id: '-k2487095', score: 1483130099994 },
//   { id: '-k2491442', score: 1483130099994 },
//   { id: '-k2479357', score: 1483130099993 },
//   { id: '-k2487094', score: 1483130099993 },
//   { id: '-k2491441', score: 1483130099993 },
//   { id: '-k2479356', score: 1483130099992 },
//   { id: '-k2487093', score: 1483130099992 },
//   { id: '-k2491440', score: 1483130099992 },
//   { id: '-k2479355', score: 1483130099991 },
//   { id: '-k2487092', score: 1483130099991 },
//   { id: '-k2491439', score: 1483130099991 },
//   { id: '-k2479354', score: 1483130099990 },
//   { id: '-k2487091', score: 1483130099990 } ]
