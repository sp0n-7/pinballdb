const cache       = require('../lib/cache');
const getCacheId  = cache.getCacheId;
const getDocId    = cache.getDocId;


// const docId     = '-Ka37q8nJvmJSHUsdx2u';
const cityCode  = 'nyc';

const t0 = Date.now();
for (let i = 0; i < 100000;i++) {
  const docId = '-K' + i;
  const sCacheId  = getCacheId({ id: docId });
  // console.log({ sCacheId: sCacheId });

  const oDocId    = getDocId(sCacheId);
  // console.log({ oDocId: oDocId });  
}
console.log('time',Date.now()-t0);

// local host, 71ms for 100k codes