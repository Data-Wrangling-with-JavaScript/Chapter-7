'use strict';

const stream = require('stream');
const openCsvInputStream = require('./toolkit/open-csv-input-stream');
const openCsvOutputStream = require('./toolkit/open-csv-output-stream');

//
// Convert the temperature for a single record.
// Converts from 'tenths of degress celcius' to 'degrees celcius'.
//
// https://earthscience.stackexchange.com/questions/5015/what-is-celsius-degrees-to-tenths
//
var transformRow = inputRow => {

    // Your code here to transform a row of data.

    const outputRow = Object.assign({}, inputRow); // Clone record, prefer not to modify source data.

    if (typeof(outputRow.MinTemp) === 'number') {
        outputRow.MinTemp /= 10;
    }
    else {
        outputRow.MinTemp = undefined;
    }

    if (typeof(outputRow.MaxTemp) === 'number') {
        outputRow.MaxTemp /= 10;
    }
    else {
        outputRow.MaxTemp = undefined;
    }

    return outputRow;
};

//
// Transform a data set (in this case a chunk of rows).
//
var transformData = inputData => {
    // Your code here to transform a batch of rows.
    return inputData.map(transformRow);
};

//
// Create a stream that converts the temperature for all records that pass through the stream.
//
var convertTemperatureStream = () => {
    const transformStream = new stream.Transform({ objectMode: true }); // Create a bidirectional stream in 'object mode'.
    transformStream._transform = (inputChunk, encoding, callback) => { // Callback to execute on chunks that are input.
        var outputChunk = transformData(inputChunk); // Transform the chunk.
        transformStream.push(outputChunk); // Pass the converted chunk to the output stream.
        callback();
    };
    return transformStream;
};

//
// Use Node.js streams to pipe the content of one CSV file to another.
//
openCsvInputStream('./data/weather-stations.csv')
    .pipe(convertTemperatureStream())
    .pipe(openCsvOutputStream('./output/transformed-csv.csv'))
    .on('error', err => {
        console.error("An error occurred while transforming the CSV file.");
        console.error(err);
    });
