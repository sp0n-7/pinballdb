const Pinball      = require('../lib/pinball');

// example
// const cityCodes    = {
//   "la" : {
//     "name" : "Los Angeles, CA",
//     "boundingRectangle": { lowerLeft: [33.40163829558248,-118.7017822265625], upperRight: [34.3366324743773,-117.13897705078125] }
//   },
//   "nyc" : {
//     "name" : "New York, NY",
//     "boundingRectangle": { lowerLeft: [40.47724766391948,-74.26277160644531], upperRight: [40.93037458898227,-73.71345520019531] }
//   }
// }

// example options
/*
{
  nyc: {
    lowerLatitude     : 40.47724766391948,
    upperLatitude     : 40.93037458898227,
    lowerLongitude    : -74.26277160644531,
    upperLongitude    : -73.71345520019531,
    NLatitude         : 40,
    NLongitude        : 40,
    NBucketThreshold  : 5000   
  },
  la: {
    lowerLatitude     : 33.40163829558248,
    upperLatitude     : 34.3366324743773,
    lowerLongitude    : -118.7017822265625,
    upperLongitude    : -117.13897705078125,
    NLatitude         : 100,
    NLongitude        : 100,
    NBucketThreshold  : 5000   
  }
}
*/

class Pinballs {
  constructor(options) {
    this.pbs = {};
    let aPromises = [];
    for (let cityCode in options) {
      const oSetting = options[cityCode];
      this.pbs[oSetting.cityCode] = new Pinball({
        cityCode          : cityCode,
        lowerLatitude     : oSetting.lowerLatitude,
        upperLatitude     : oSetting.upperLatitude,
        lowerLongitude    : oSetting.lowerLongitude,
        upperLongitude    : oSetting.upperLongitude,
        NLatitude         : oSetting.NLatitude,
        NLongitude        : oSetting.NLongitude,
        NBucketThreshold  : oSetting.NBucketThreshold
      });
    }
  }

  // sample options, the live / hot redis url
  // {
  //   nyc: {
  //     sCacheUrl: 'redis://user:pass@someHost:somePort'
  //   },
  //   la: {
  //     sCacheUrl: 'redis://user:pass@someHost:somePort'
  //   }
  // }
  addSubscriber(options) {
    let aPromises = [];
    for (let cityCode in options) {
      const sCacheUrl = options[cityCode].sCacheUrl;
      aPromises.push(this.pbs[cityCode].addSubscriber({ sCacheUrl: sCacheUrl }));
    }
    return Promise.all(aPromises);
  }

}


module.export = Pinballs