# pinballDB

kind of a 3D in memory document db. ~~so far it's slow~~, and needs work (testing!).

in memory cache for documents querying by lat, lon boundaries, and time sorted
  * kv store & ordered array by score (timestamp)
  * query seeks backward from end of array and finds all values in region
  * medium/large overlapping region scans can find recent events fast (quicker)
  * tiny boundaries within region with no matches scans through entire collection (slower)
  * single process (have not setup node cluster yet, should be easy to do so for queries)



hybrid backwards time search algorithm, 100k docs, 100k queries made

```
// explored 40x40,20x20,10x10
// works in conjuction with NBucketThreshold the algorithm switch
//   if N total within buckets > threshold does full scan backwards on ordered array of events
//   else it takes all bucket arrays, combines, sorts and keeps N highest (faster than select N tree methods explored)
// if the most likely query is large, smaller bucket dims work faster, due to quicker intermediate grid sums
const NLat = 10;
const NLon = 10;
const NBucketThreshold = 5000;
const halfWinLonScale = 0.04;
const halfWinLatScale = 0.04;

```

```
load time ~ 1.1seconds for 100k elements, 100k queries made, single process

0 size query windows lat lon space (pinhole)
{ queriesTimeMS: 4477, queriesPerSecond: 22336.38597274961 }

query halfwindow range 0-0.001 lat lon space (very small search windows)
{ queriesTimeMS: 13400, queriesPerSecond: 7462.686567164179 }


query halfwindow range 0-0.02 lat lon space (small search windows)
{ queriesTimeMS: 7256, queriesPerSecond: 13781.697905181918 }

messel@messels-MBP:~/Desktop/Dropbox/code/js/db_tuts/pinball_tut/test$ node query.js 
{ queriesTimeMS: 4171, queriesPerSecond: 23975.06593143131 }
```
  
  
