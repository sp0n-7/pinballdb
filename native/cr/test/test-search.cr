# require "json" # to use JSON::Any
require "../lib/util"

anArray = [] of Hash(Symbol, String | Int32 | Int64 | Float64)

N = 20
i = 0
while i < N
  val = i
  if i % 2
    val = i - 1
  end
  oHash = {:id => "-k#{i}", :val => val}
  anArray.push(oHash)
  puts anArray[anArray.size - 1][:val]
  i += 1
end

i = 0
while i < 2*N
  val = i - N
  index = binarySearch(anArray, val)
  puts index
  # anArray.splice(index,0, oItem)
  # puts(index,anArray);
  i += 1
end
