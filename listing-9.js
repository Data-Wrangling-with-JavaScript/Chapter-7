'use strict';

var MongoClient = require('mongodb').MongoClient;
MongoClient.connect('mongodb://localhost')
    .then(client => {
        var db = client.db('weather_stations');
        var collection = db.collection('daily_readings');
        var query = {}; // Retreive all records.
        var projection = { // This defines the fields to retreive from each record.
            fields: {
                _id: 0,
                Year: 1,
                Month: 1,
                Day: 1,
                Precipitation: 1
            }
        };
        return collection.find(query, projection) // Retreive only specified fields.
            .toArray()
            .then(data => {
                console.log(data);
            })
            .then(() => client.close()); // Close database when done.
    })
    .then(() => {
        console.log("Done.");
    })
    .catch(err => {
        console.error("An error occurred reading the database.");
        console.error(err);
    });
