'use strict';

var numRecords = 0;

//
// Read the entire database, document by document using a database cursor.
//
var readDatabase = cursor => {
    return cursor.next()
        .then(record => {
            if (record) {
                // Found another record.
                console.log(record);
                ++numRecords;

                // Read the entire database using an asynchronous recursive traversal.
                return readDatabase(cursor);
            }
            else {
                // No more records.
            }
        });
};

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
        return readDatabase(db.collection.find()) // NOTE: You could use a query here.
            .then(() => db.close()); // Close database when done.
    })
    .then(() => {
        console.log("Displayed " + numRecords + " records.");
    })
    .catch(err => {
        console.error("An error occurred reading the database.");
        console.error(err);
    });
