'use strict';

var numRecords = 0;
var numPages = 0;

// 
// Read a single page of data from the database.
//
var readPage = (collection, pageIndex, pageSize) => {
    var skipAmount = pageIndex * pageSize;
    var limitAmount = pageSize;
    return collection.find()
        .skip(skipAmount)
        .limit(pageSize)
        .toArray();
};

// 
// Read the entire database, page by page.
//
var readDatabase = (collection, startPageIndex, pageSize) => {
    return readPage(collection, startPageIndex, pageSize)
        .then(data => {
            if (data.length > 0) {
                // We got some data back.
                console.log('chunk: ' + data.length);
                console.log(data);

                numRecords += data.length;
                ++numPages;

                // Read the entire database using an asynchronous recursive traversal.
                return readDatabase(collection, startPageIndex+1, pageSize);
            }
            else {
                // We retreived no data, finished reading.
            }
        })
    
};

//
// Open the connection to the database.
//
let openDatabase = () => {
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
		var pageSize = 100;
        return readDatabase(db.collection, 0, pageSize)
            .then(() => {
                return db.close(); // Close database when done.
            });
    })
    .then(() => {
        console.log("Displayed " + numRecords + " records in " + numPages + " pages.");
    })
    .catch(err => {
        console.error("An error occurred reading the database.");
        console.error(err);
    });
