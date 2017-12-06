'use strict';

const stream = require('stream');
const fs = require('fs');
const papaparse = require('papaparse');

//
// Open a streaming CSV file for output.
//
var openCsvOutputStream = outputFilePath => {

    let firstOutput = true;
    const fileOutputStream = fs.createWriteStream(outputFilePath); // Create stream for writing the output file.
    
    const csvOutputStream = new stream.Writable({ objectMode: true }); // Create stream for writing data records, note that 'object mode' is enabled.
    csvOutputStream._write = (chunk, encoding, callback) => { // Handle writes to the stream.
        var outputCSV = papaparse.unparse(chunk, { 
            header: firstOutput
        });
        fileOutputStream.write(outputCSV + '\n');
        firstOutput = false; 
        callback();        
    };
    csvOutputStream._destroy = () => { // When the CSV stream is closed, close the output file stream.
        fileOutputStream.end();
    };

    return csvOutputStream;
};

module.exports = openCsvOutputStream;