const Pinball      = require('../lib/pinball');
const Trending     = require('../lib/trending');

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
    this.trs = {};
    let aPromises = [];
    for (let cityCode in options) {
      const oSetting = options[cityCode];
      this.pbs[cityCode] = new Pinball({
        cityCode          : cityCode,
        lowerLatitude     : oSetting.lowerLatitude,
        upperLatitude     : oSetting.upperLatitude,
        lowerLongitude    : oSetting.lowerLongitude,
        upperLongitude    : oSetting.upperLongitude,
        NLatitude         : oSetting.NLatitude,
        NLongitude        : oSetting.NLongitude,
        NBucketThreshold  : oSetting.NBucketThreshold
      });
      this.trs[cityCode] = new Trending({
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
  //     sCacheUrl: 'redis://user:pass@someHost:somePort',
  //     aProps:    ['id','latitude','longitude','score','cs','ts', 'll'] // optional pluck arg
  //   },
  //   la: {
  //     sCacheUrl: 'redis://user:pass@someHost:somePort',
  //     aProps:    ['id','latitude','longitude','score','cs','ts', 'll']
  //   }
  // }
  addSubscriber(aOptions) {
    let aPromises = [];
    for (let cityCode in aOptions) {
      const options = aOptions[cityCode];
      aPromises.push(this.pbs[cityCode].addSubscriber(options));
      aPromises.push(this.trs[cityCode].addSubscriber(options));
    }
    return Promise.all(aPromises);
  }

}


module.exports = Pinballs;
