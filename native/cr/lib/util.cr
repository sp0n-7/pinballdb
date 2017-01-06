# modified from http://oli.me.uk/2013/06/08/searching-javascript-arrays-with-a-binary-search/
# finds an insertion point for the given value
def binarySearch(aArray : Array, val : Int32 | Int64 | Float64)
  # searchElement
  currentIndex = -1
  minIndex = 0
  maxIndex = aArray.size - 1

  while minIndex <= maxIndex
    currentIndex = ((minIndex + maxIndex) / 2).to_i64
    currentVal = aArray[currentIndex][:val]

    if currentVal < val
      minIndex = currentIndex + 1
    elsif currentVal > val
      maxIndex = currentIndex - 1
    else
      return currentIndex
    end
  end

  # had to nudge the final returned index up by one if min and max met 1 before
  if currentIndex >= 0 && aArray[currentIndex][:val] < val
    currentIndex += 1
  end

  return currentIndex
end

def mergeSortedArrays(a : Array, b : Array, sProperty : String)
  aMerged = [] of Hash(Symbol, String | Int32 | Int64 | Float64)
  i = 0, j = 0, k = 0

  while i < a.size && j < b.size
    if a[i][sProperty] < b[j][sProperty]
      aMerged[k] = a[i]
      i += 1
    else
      aMerged[k] = b[j]
      j += 1
    end
    k += 1
  end

  while i < a.size
    aMerged[k] = a[i]
    k += 1
    i += 1
  end

  while j < b.size
    aMerged[k] = b[j]
    k += 1
    j += 1
  end

  return aMerged
end
