# pinballDB

kind of a 3D in memory document db. ~~so far it's slow~~, and needs work (testing!).

in memory cache for documents querying by lat, lon boundaries, and time sorted
  * kv store & ordered array by score (timestamp)
  * determines from grid whether to do a full scan or grid bucket scan
  * medium/large overlapping region scans can find highest score events in microseconds
    * query seeks backward from end of array and finds all values in region
  * tiny/small boundaries within region use grid buckets to match in a millisecond or less
    * query creates heap from nearby grid contents, sorts and returns
  * single process - sharing read access to the memory doesn't jive with node cluster module well

next steps?:
  * convert core functionality to a lower level language, even faster per process performance
  * utilize read threads for queries to support multiple cores


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

query halfwindow range 0-0.001 lat lon space (tight search windows)
{ queriesTimeMS: 13400, queriesPerSecond: 7462.686567164179 }


query halfwindow range 0-0.02 lat lon space (medium search windows)
{ queriesTimeMS: 7256, queriesPerSecond: 13781.697905181918 }

query halfwindow range 0-0.04 lat lon space (large search windows)
messel@messels-MBP:~/Desktop/Dropbox/code/js/db_tuts/pinball_tut/test$ node query.js 
{ queriesTimeMS: 4171, queriesPerSecond: 23975.06593143131 }
```
  
  
