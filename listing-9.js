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
        return db.collection.find(query, projection) // Retreive only specified fields.
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
