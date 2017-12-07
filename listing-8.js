'use strict';

var MongoClient = require('mongodb').MongoClient;
MongoClient.connect('mongodb://localhost')
    .then(client => {
        var db = client.db('weather_stations');
        var collection = db.collection('daily_readings');
        var query = { // Define our database query
            Year: {
                $gte: 2000, // Year >= 2000
            },
        };
        return collection.find(query) // Retreive records since the year 2000.
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
