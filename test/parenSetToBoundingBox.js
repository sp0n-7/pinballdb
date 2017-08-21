const sParenCoords = '(-34.45335087852228, -58.525543212890625)(-34.50881994305569, -58.709564208984375)(-34.64337687054745, -58.69720458984375)(-34.734841371777705, -58.58734130859375)(-34.78335551870688, -58.311309814453125)(-34.64676624651909, -58.201446533203125)'

let a0 = sParenCoords.split(/[(),]/);
let a1 = [];
for (let i=0;i < a0.length;i++) {
  const val = a0[i];
  if (val.length > 0) {
    a1.push(parseFloat(val));
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
