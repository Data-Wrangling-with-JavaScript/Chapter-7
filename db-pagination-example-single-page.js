'use strict';

// 
// Read a single page of data from the database.
//
var readPage = (collection, pageIndex, pageSize) => {
    var skipAmount = pageIndex * pageSize;
    var limitAmount = pageSize;
    return collection.find().skip(skipAmount).limit(pageSize).toArray();
};

var MongoClient = require('mongodb').MongoClient;
MongoClient.connect('mongodb://localhost')
    .then(client => {
        var db = client.db('weather_stations');
        var collection = db.collection('daily_readings');
        return readPage(collection, 2, 10)
            .then(data => {
                console.log(data);

                return client.close(); // Close database when done.
            });
    })
    .catch(err => {
        console.error("An error occurred reading the database.");
        console.error(err);
    });
