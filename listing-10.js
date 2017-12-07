'use strict';

//
// Open the connection to the database.
//
function openDatabase () {
    var MongoClient = require('mongodb').MongoClient;
    return MongoClient.connect('mongodb://localhost')
        .then(client => {
            var db = client.db('weather_stations');
            var collection = db.collection('daily_readings');
            return {
                collection: collection,
                close: () => {
                    return client.close();
                },
            };
        });
};

openDatabase()
    .then(db => {
        return db.collection.find() // Retreive only specified fields.
            .sort({
                Precipitation: 1
            })
            .toArray()
            .then(data => {
                console.log(data);
            })
            .then(() => db.close()); // Close database when done.
    })
    .then(() => {
        console.log("Done.");
    })
    .catch(err => {
        console.error("An error occurred reading the database.");
        console.error(err);
    });
