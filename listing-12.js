'use strict';

const argv = require('yargs').argv;
var MongoClient = require('mongodb').MongoClient;
var spawn = require('child_process').spawn;
var parallel = require('async-await-parallel');

//
// Run the slave process.
//
var runSlave = (skip, limit) => {
    return new Promise((resolve, reject) => {
        var args = [
            'parallel-slave-example.js', 
            '--skip', 
            skip,
            '--limit',
            limit
        ];
        console.log("$$ node " + args.join(' '));
        const childProcess = spawn('node', args);
        childProcess.stdout.on('data', data => {
            console.log(`stdout: ${data}`);
        });

        childProcess.stderr.on('data', data => {
            console.log(`stderr: ${data}`);
        });

        childProcess.on('close', code => {
            resolve();
        });

        childProcess.on('close', code => {
            reject();
        });
    });
};

//
// Run the slave process for a particular batch of records.
//
var processBatch = (batchIndex, batchSize) => {
    var startIndex = batchIndex * batchSize;
    return () => { // Encapsulate in an anon fn so that execution is deferred until later.
        return runSlave(startIndex, batchSize);
    };
};

//
// Process the entire database in batches of 100 records.
// 2 batches are processed in parallel, but this number can be tweaked based on the number of cores you
// want to throw at the problem.
//
var processDatabaseInBatches = (numRecords) => {

    var batchSize = 100; // The number of records to process in each batchs.
    var maxProcesses = 2; // The number of process to run in parallel.
    var numBatches = numRecords / batchSize; // Total nujmber of batches that we need to process.
    var slaveProcesses = [];
    for (var batchIndex = 0; batchIndex < numBatches; ++batchIndex) {
        slaveProcesses.push(processBatch(batchIndex, batchSize));
    }

    return parallel(slaveProcesses, maxProcesses);
};

MongoClient.connect('mongodb://localhost')
    .then(client => {
        var db = client.db('weather_stations');
        var collection = db.collection('daily_readings');
        collection.find().count() // Determine number of records to process.
            .then(numRecords => processDatabaseInBatches(numRecords)) // Process the entire database.
            .then(() => client.close()); // Close the database when done.
    })
    .then(() => {
        console.log("Done processing all records.");
    })
    .catch(err => {
        console.error("An error occurred reading the database.");
        console.error(err);
    });

