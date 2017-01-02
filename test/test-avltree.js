var BinarySearchTree = require('binary-search-tree').BinarySearchTree
  , AVLTree = require('binary-search-tree').AVLTree   // Same API as BinarySearchTree


function compareKeys (a, b) {
  if (a.val < b.val) { return -1; }
  if (a.val > b.val) { return 1; }
  return 0;
}

function checkValueEquality (a, b) {
  return a.id === b.id;
}  

// Creating a binary search tree
var bst = new AVLTree({ compareKeys: compareKeys, checkValueEquality: checkValueEquality });

// Inserting some data
bst.insert({ val: 15.5373 }, { id: 'some data for key 15.5373' });
bst.insert({ val: 12.2256 }, { id: 'something else' });
bst.insert({ val: 18.6197 }, { id: 'hello' });

// You can insert multiple pieces of data for the same key
// if your tree doesn't enforce a unique constraint
bst.insert({ val: 18.6197 }, { id: 'world'});

// Retrieving data (always returned as an array of all data stored for this key)
console.log('search 15.5373',bst.search({ val: 15.5373 }));   // Equal to ['some data for key 15']
console.log('search 18.6197',bst.search({ val: 18.6197 }));   // Equal to ['hello', 'world']
console.log('search 1.1312',bst.search({ val: 1.1312 }));    // Equal to []

// Search between bounds with a MongoDB-like query
// Data is returned in key order
// Note the difference between $lt (less than) and $gte (less than OR EQUAL)
console.log('between [12.2256-18.6197)',bst.betweenBounds({ $lt: { val: 18.6197 }, $gte: { val: 12.2256 }}));   // Equal to ['something else', 'some data for key 15']

// Deleting all the data relating to a key
bst.delete({ val: 15.5373 });   // bst.search(15) will now give []
bst.delete({ val: 18.6197 }, { id: 'world' });   // bst.search(18) will now give ['hello'

console.log('search 18.6197 after delete world',bst.search({ val: 18.6197 }));