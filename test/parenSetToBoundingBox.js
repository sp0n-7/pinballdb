// const sParenCoords = '(-34.45335087852228, -58.525543212890625)(-34.50881994305569, -58.709564208984375)(-34.64337687054745, -58.69720458984375)(-34.734841371777705, -58.58734130859375)(-34.78335551870688, -58.311309814453125)(-34.64676624651909, -58.201446533203125)'

// let a0 = sParenCoords.split(/[(),]/);
// let a1 = [];
// for (let i=0;i < a0.length;i++) {
//   const val = a0[i];
//   if (val.length > 0) {
//     a1.push(parseFloat(val));
//   }
// }
// console.log(a1);

const aLonLat = [
  -122.5077724, 37.7066400,
  -122.3716450, 37.7076585,
  -122.3580837, 37.7133623,
  -122.3524189, 37.7286380,
  -122.3708725, 37.7471004,
  -122.3810863, 37.7853681,
  -122.3728466, 37.7996795,
  -122.3538780, 37.8093771,
  -122.3574829, 37.8290396,
  -122.3724174, 37.8355475,
  -122.3823738, 37.8318869,
  -122.3842621, 37.8264635,
  -122.3761940, 37.8162935,
  -122.3737907, 37.8080209,
  -122.3868370, 37.7956780,
  -122.4040031, 37.8131744,
  -122.4247742, 37.8135812,
  -122.4464035, 37.8103264,
  -122.4493217, 37.8097840,
  -122.4472618, 37.7926937,
  -122.4668312, 37.7884884,
  -122.4874306, 37.7915407,
  -122.5059700, 37.7886919,
  -122.5162697, 37.7812980,
  -122.5136948, 37.7671866,
  -122.5077724, 37.7066400
]

let a1 = []
for (let i in aLonLat) {
  if (i%2) {
    console.log(i,aLonLat[i-1],aLonLat[i])
    a1[i-1] = aLonLat[i]
    a1[i]   = aLonLat[i-1]
  }  
}
console.log(a1);

// bounding box

if (a1.length > 0) {
  let llLat = a1[0];
  let llLon = a1[1];
  let urLat = a1[0];
  let urLon = a1[1];

  for (let i=0;i < a1.length;i++) {
    const val = a1[i];
    if (i%2) {
      if (val < llLon) llLon = val;
      if (val > urLon) urLon = val;
    }
    else {
      if (val < llLat) llLat = val;
      if (val > urLat) urLat = val;
    }
  }

  console.log(`"boundingRectangle": { "lowerLeft": [${llLat},${llLon}], "upperRight": [${urLat},${urLon}] }`)
}
