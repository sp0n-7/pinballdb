# pinballDB

kind of a 3D in memory document db. so far it's slow, and needs work.

in memory cache for documents querying by lat, lon boundaries, and time sorted
  * shoves stuff into a grid
  * groups results and grabs the N highest 



brute force backwards time search, 100k docs, 100k queries made

  messel@messels-MBP:~/Desktop/Dropbox/code/js/db_tuts/pinball_tut/test$ node query.js 
  load time 6735
  { queriesTimeMS: 4441, queriesPerSecond: 22517.45102454402 }
  -k99990 1483286864651
  -k99982 1483286864643
  -k99980 1483286864641
  -k99979 1483286864640
  -k99972 1483286864633
  -k99966 1483286864627
  -k99964 1483286864625
  -k99963 1483286864624
  -k99959 1483286864620
  -k99958 1483286864619
  -k99957 1483286864618
  -k99950 1483286864611
  -k99949 1483286864610
  -k99947 1483286864608
  -k99940 1483286864601
  -k99937 1483286864598
  -k99923 1483286864584
  -k99910 1483286864571
  -k99900 1483286864561
  -k99896 1483286864557
  
  
  
