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

class Pinballs {
  constructor(options) {
    const oSettings = options.oSettings;
    this.pbs = {};
    let aPromises = [];
    for (let cityCode in oSettings) {
      const oSetting = oSettings[cityCode];
      this.pbs[oSetting.cityCode] = new Pinball({
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

  addSubscriber(options) {
    const sAction   = Pinball.name + '.addSubscriber'; 
    const sCacheUrl = options.sCacheUrl;
    let aPromises = [];
    for (let cityCode in oSettings) {
      aPromises.push(pb.addSubscriber({ sCacheUrl: sCacheUrl, cityCode: cityCode }));
    }
    return Promise.all(aPromises);
  }

  loadFromCache(options) {
    const sCacheUrl = options.sCacheUrl;
    let aPromises = [];
    for (let cityCode in this.pbs) {
      aPromises.push(pb.loadFromCache({ sCacheUrl: sCacheUrl, cityCode: cityCode }));
    }
    return Promise.all(aPromises);
  }

}


module.export = Pinballs