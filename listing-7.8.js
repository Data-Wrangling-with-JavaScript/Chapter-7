'use strict';

const stream = require('stream');
const openJsonInputStream = require('./toolkit/open-json-input-stream.js');
const openJsonOutputStream = require('./toolkit/open-json-output-stream.js');

const inputFilePath = "./data/weather-stations.json";
const outputFilePath = "./output/weather-stations-ftransformed.json";

//
// Convert the temperature for a single record.
// Converts from 'tenths of degress celcius' to 'degrees celcius'.
//
// https://earthscience.stackexchange.com/questions/5015/what-is-celsius-degrees-to-tenths
//
function transformRow (inputRow) {

    // Your code here to transform a row of data.

    const outputRow = Object.assign({}, inputRow); // Clone record, prefer not to modify source data.

    if (typeof(outputRow.MinTemp) === "number") {
        outputRow.MinTemp /= 10;
    }
    else {
        outputRow.MinTemp = undefined;
    }

    if (typeof(outputRow.MaxTemp) === "number") {
        outputRow.MaxTemp /= 10;
    }
    else {
        outputRow.MaxTemp = undefined;
    }

    return outputRow;
};

//
// Create a stream that converts the temperature for all records that pass through the stream.
//
function convertTemperatureStream () {
    const transformStream = new stream.Transform({ objectMode: true }); // Create a bidirectional stream in 'object mode'.
    transformStream._transform = (inputChunk, encoding, callback) => { // Callback to execute on chunks that are input.
        var outputChunk = transformRow(inputChunk); // Transform the chunk.
        transformStream.push(outputChunk); // Pass the converted chunk to the output stream.
        callback();
    };
    return transformStream;
};

//
// Use Node.js streams to pipe the content of one JSON file to another, transforming the data on the way through.
//
openJsonInputStream(inputFilePath)
    .pipe(convertTemperatureStream())
    .pipe(openJsonOutputStream(outputFilePath))
    .on("error", err => {
        console.error("An error occurred while transforming the JSON file.");
        console.error(err);
    });
