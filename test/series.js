
const series = aPfunc => {
  let result = Promise.resolve([]);
  aPfunc.forEach( pFunc => {
    result = result.then( aResults => {
      return pFunc().then( data => {
        aResults.push(data);
        return aResults 
      })
    });
  })
  return result;
}

// const serial = funcs =>
//     funcs.reduce((promise, func) =>
//         promise.then(result => func().then(
//           Array.prototype.concat.bind(result))), Promise.resolve([]))

let aPfunc = [];

aPfunc.push( () => {
  return new Promise( (resolve,reject) => {
    const dT = Math.floor(Math.random()*1000);
    console.log('delay 0',dT)
    setTimeout(resolve(0),dT);
  })
})

aPfunc.push( () => {
  return new Promise( (resolve,reject) => {
    const dT = Math.floor(Math.random()*1000);
    console.log('delay 1',dT)
    setTimeout(resolve(1),dT);
  })
})

aPfunc.push( () => {
  return new Promise( (resolve,reject) => {
    const dT = Math.floor(Math.random()*1000);
    console.log('delay 2',dT)
    setTimeout(resolve(2),dT);
  })
})

series(aPfunc).then( aResults => {
  console.log(aResults);
})
.catch( err => {
  console.error(err);
})