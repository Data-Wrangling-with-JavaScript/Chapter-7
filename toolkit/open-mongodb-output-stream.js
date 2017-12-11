'use strict';

const stream = require('stream');
var MongoClient = require('mongodb').MongoClient;

//
// Open a streaming CSV file for output.
//
function openMongodbOutputStream (config) {

    var connection = null; // Connection to database, once connected.
    var db = null;  // Database to store data into, once connected.
    var collection = null; // Collection to store data into, once connected.

    var hostName = config.host; // Name of host to connect to. 
    var databaseName = config.database; // Name of database to store data.
    var collectionName = config.collection; // Name of collection in database to store data.

    //
    // Connect to the database (if not already connected) and insert a chunk of data.
    //
    var connectAndInsert = (chunk) => { 
        if (!db) {
            // Database not connected yet.
            return MongoClient.connect(hostName) // Connect to the database.
                .then(client => {
                    connection = client;
                    db = client.db(databaseName); // Save the connected database.
                    collection = db.collection(collectionName); // Save the collection we are writing to.
                    return collection.insertMany(chunk); // Insert the chunk of data.
                });
        }
        else {
            // Already connected to database, just insert the chunk of data.
            return collection.insertMany(chunk);
        }
    };

    const csvOutputStream = new stream.Writable({ objectMode: true }); // Create stream for writing data records, note that 'object mode' is enabled.
    csvOutputStream._collectedChunks = [];
    csvOutputStream._write = (chunk, encoding, callback) => { // Handle writes to the stream.
        connectAndInsert(chunk)
            .then(() => {
                callback(); // Successfully added to database.
            })
            .catch(err => {
                callback(err); // An error occurred, pass it onto the stream.
            });
    };

    csvOutputStream.on('finish', () => { // When the CSV stream is finished, close the connection to the database.
        if (connection) {
            connection.close();
            connection = null;
            db = null;
            collection = null;
        }
    });

    return csvOutputStream;
};

module.exports = openMongodbOutputStream;