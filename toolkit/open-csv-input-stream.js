'use strict';

const stream = require('stream');
const fs = require('fs');
const papaparse = require('papaparse');

//
// Open a streaming CSV file for input.
//
function openCsvInputStream (inputFilePath) {

    const csvInputStream = new stream.Readable({ objectMode: true }); // Create a stream that we can read data records from, note that 'object mode' is enabled.
    csvInputStream._read = () => {}; // Must include, otherwise we get an error.

    const fileInputStream = fs.createReadStream(inputFilePath); // Create stream for reading the input file.
    papaparse.parse(fileInputStream, {
        header: true,     
        dynamicTyping: true,

        // We may not need this, but don't want to get halfway through the massive file before realising it is needed.
        skipEmptyLines: true, 

        step: (results) => { // Handles incoming rows of CSV data.
            for (let row of results.data) {
                csvInputStream.push(row); // Push results as they are streamed from the file.                
            }
        },

        complete: () => { // File read operation has completed.
            csvInputStream.push(null); // Signify end of stream.
        },

        error: (err) => { // An error has occurred.
            csvInputStream.emit("error", err); // Pass on errors.
        }
    });    

    return csvInputStream;
};

module.exports = openCsvInputStream;