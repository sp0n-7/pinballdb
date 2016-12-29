'use strict';

const geolib = require('geolib');


const p0 = [51.5103,7.49347];
const p1 = [51.5203,7.49347];

const dm = geolib.getDistance(
    { latitude: p0[0], longitude: p0[1] },
    { latitude: p1[0], longitude: p1[1] }
);
const dmi = geolib.convertUnit('mi', dm, 2);

console.log({ p0: p0, p1: p1, distanceMeters: dm, distanceMiles: dmi });