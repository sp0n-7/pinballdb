# pinballDB

kind of a 3D in memory document db. ~~so far it's slow~~, and needs work (bug for internal sorts).

in memory cache for documents querying by lat, lon boundaries, and time sorted
  * kv store & ordered array by score (timestamp)
  * query seeks backward from end of array and finds all values in region
  * medium/large overlapping region scans can find recent events fast (quicker)
  * tiny boundaries within region with no matches scans through entire collection (slower)
  * single process (have not setup node cluster yet, should be easy to do so for queries)



brute force backwards time search, 100k docs, 100k queries made
```

0 size query windows lat lon space (worst case)


query halfwindow range 0-0.02 lat lon space (small search windows)
messel@messels-MBP:~/Desktop/Dropbox/code/js/db_tuts/pinball_tut/test$ node query.js 
load time 1235
{ queriesTimeMS: 11382, queriesPerSecond: 8785.802143735724 }

queries, query halfwindow range 0-0.04 lat lon space (larger search windows)
messel@messels-MBP:~/Desktop/Dropbox/code/js/db_tuts/pinball_tut/test$ node query.js 
load time 1238
{ queriesTimeMS: 3951, queriesPerSecond: 25310.048089091368 }


```
  
  
