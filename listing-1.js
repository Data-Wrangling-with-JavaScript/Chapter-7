'use strict';

const papaparse = require('papaparse');
const fs = require('fs');
const stream = require('stream');

var transformRow = inputRow => {
    // Your code here to transform a record.
    return inputRow;
}

var transformData = inputData => {
    // Your code here to transform a batch of records.
    //TODO: sort this out!
    return inputData.map(inputRow);
};

//
// Open a streaming CSV file for input.
//
var openCsvInputStream = inputFilePath => {

    const csvInputStream = new stream.Readable({ objectMode: true });
    csvInputStream._read = () => {}; // Must include, otherwise we get an error.

    const fileInputStream = fs.createReadStream(inputFilePath);
    papaparse.parse(fileInputStream, {
        header: true,     
        dynamicTyping: true,

        // We may not need this, but don't want to get halfway through the massive file before realising it is needed.
        skipEmptyLines: true, 

        step: (results) => {
            csvInputStream.push(results.data); // Push results as they are streamed from the file.
        },

        complete: () => {
            csvInputStream.push(null); // Signify end of stream.
        },

        error: (err) => {
            csvInputStream.emit('error', err);
        }
    });    

    return csvInputStream;
};

//
// Open a streaming CSV file for output.
//
var openCsvOutputStream = outputFilePath => {

    let firstOutput = true;
    let fileOutputStream = fs.createWriteStream(outputFilePath);
    
    let csvOutputStream = new stream.Writable({ objectMode: true });
    csvOutputStream._write = (chunk, encoding, callback) => {
        var outputCSV = papaparse.unparse(chunk, { 
            header: firstOutput
        });
        fileOutputStream.write(outputCSV + '\n');
        firstOutput = false; 
        callback();        
    };
    csvOutputStream._destroy = () => {
        fileOutputStream.end();
    };

    return csvOutputStream;
};

//
// Convert the temperature for a single record.
// Converts from 'tenths of degress celcius' to 'degrees celcius'.
//
// https://earthscience.stackexchange.com/questions/5015/what-is-celsius-degrees-to-tenths
//
var convertTemperature = inputRecord => {

    const outputRecord = Object.assign({}, inputRecord); // Clone record, prefer not to modify source data.

    if (typeof(outputRecord.MinTemp) === 'number') {
        outputRecord.MinTemp /= 10;
    }
    else {
        outputRecord.MinTemp = undefined;
    }

    if (typeof(outputRecord.MaxTemp) === 'number') {
        outputRecord.MaxTemp /= 10;
    }
    else {
        outputRecord.MaxTemp = undefined;
    }

    return outputRecord;
};

//
// Create a stream that converts the temperature for all records that pass through the stream.
//
var convertTemperatureStream = () => {
    const transformStream = new stream.Transform({ objectMode: true });
    transformStream._transform = (inputChunk, encoding, callback) => { // Callback to execute on chunks that are input.
        var outputChunk = inputChunk.map(convertTemperature);
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
