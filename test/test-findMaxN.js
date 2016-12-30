const findMaxN = require('../lib/util').findMaxN;

const aArraySet = [
  [],
  [
    { id: 'a', score: 0    },
    { id: 'b', score: 3    },
    { id: 'c', score: 5.4  },
    { id: 'd', score: 5.4  },
    { id: 'e', score: 13   } 
  ],
  [
    { id: 'f', score: 3.4  },
    { id: 'g', score: 3.41 },
    { id: 'h', score: 3.42 },
    { id: 'i', score: 3.43 },
    { id: 'j', score: 15   } 
  ],
  null,
  [
    { id: 'k', score: 0    },
    { id: 'l', score: 1    },
    { id: 'm', score: 2    },
    { id: 'n', score: 3    },
    { id: 'o', score: 4    } 
  ],
  [
    { id: 'p', score: 5    },
    { id: 'q', score: 6    },
    { id: 'r', score: 7.4  },
    { id: 's', score: 8.4  },
    { id: 't', score: 13   } 
  ],
];

let aResults = findMaxN({ aArraySet: aArraySet, N: 5 });
console.log('aResults',aResults);

// answer should be
// aResults [ { id: 'j', score: 15 },
//   { id: 'e', score: 13 },
//   { id: 't', score: 13 },
//   { id: 's', score: 8.4 },
//   { id: 'r', score: 7.4 } ]