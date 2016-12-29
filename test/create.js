'use strict';

//lets require/import the mongodb native drivers.
const mongodb = require('mongodb');

//We need to work with "MongoClient" interface in order to connect to a mongodb server.
const MongoClient = mongodb.MongoClient;

// Connection URL. This is where your mongodb server is running.
const url = 'mongodb://localhost:27017/test';

// Use connect method to connect to the Server
MongoClient.connect(url, (err, db) => {
  if (err) {
    console.log('Unable to connect to the mongoDB server. Error:', err);
  } 
  else {
    //HURRAY!! We are connected. :)
    console.log('Connection established to', url);

    // Get the documents collection
    let collection = db.collection('incidents');


    //Create some data, don't forget mongo loc is always lon,lat
    const N         = 100;
    const center    = [-73.993549, 40.727248];
    const lowerLeft = [-74.009180, 40.716425];
    const deltaLon  = Math.abs(lowerLeft[0] - (-73.97725));
    const deltaLat  = Math.abs(lowerLeft[1] - (40.7518692));
    let   tPrevious = 1475431264754;

    let aData = [];
    for (let i = 0; i < N; i++) {
      const incidentLon = lowerLeft[0] + Math.random() * deltaLon;
      const incidentLat = lowerLeft[1] + Math.random() * deltaLat;
      tPrevious         += Math.random() * 60 * 1000; // random time after previous
      const oIncident = { incidentId: '-k'+i, loc: [incidentLon,incidentLat], ts: tPrevious };
      aData.push(oIncident);
    }

    const showAll = () => {
      // find an incident
      collection.find({}).toArray()
      .then( (result) => {
        if (result.length) {
          console.log('Found:', result);
        } 
        else {
          console.log('No document(s) found with defined "find" criteria!');
        }
      })
      .catch( (err) => {
        console.error(err);
      });      
    }

    // showAll();

    // Insert data
    collection.insert(aData).then( (result) => {
      console.log(`Inserted ${result.length} documents into the "incidents" collection. The documents inserted with "_id" are:`, result);
    })
    .catch( (err) => {
      console.error(err);
    })
  }
});