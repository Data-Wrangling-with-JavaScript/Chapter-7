'use strict;'

const fs = require('fs');
const stream = require('stream');

//
// Open a streaming JSON file for output.
//
function openJsonOutputStream (outputFilePath) {

    const fileOutputStream = fs.createWriteStream(outputFilePath);
    fileOutputStream.write("[");

    var numRecords = 0;
    
    const jsonOutputStream = new stream.Writable({ objectMode: true });
    jsonOutputStream._write = (chunk, encoding, callback) => {
        if (numRecords > 0) {
            fileOutputStream.write(",");
        }

        // Output a single row of a JSON array.
        const jsonData = JSON.stringify(curObject);
        fileOutputStream.write(jsonData);
        numRecords += chunk.length;
        callback();        
    };

    jsonOutputStream.on('finish', () => { // When the CSV stream is finished, close the output file stream.
        fileOutputStream.write("]");
        fileOutputStream.end();
    });

    return jsonOutputStream;
};

module.exports = openJsonOutputStream;