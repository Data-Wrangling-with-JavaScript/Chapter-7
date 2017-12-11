'use strict;'

const fs = require('fs');
const stream = require('stream');

//
// Open a streaming JSON file for output.
//
function openJsonOutputStream (outputFilePath) {

    const fileOutputStream = fs.createWriteStream(outputFilePath);
    fileOutputStream.write("[\n");

    var numRecords = 0;
    
    const jsonOutputStream = new stream.Writable({ objectMode: true });
    jsonOutputStream._write = (chunk, encoding, callback) => {
        if (numRecords > 0) {
            fileOutputStream.write(",\n");
        }

        // Output a single row of a JSON array.
        // Note: don't include indentation when working a big file.
        // I only include indentation here when testing the code on a small file.
        const jsonData = JSON.stringify(curObject, null, 4);  //TODO: get rid of indentation
        fileOutputStream.write(jsonData + '\n');
        numRecords += chunk.length;
        callback();        
    };
    jsonOutputStream._destroy = () => {
        fileOutputStream.end();
    };

    return jsonOutputStream;
};

module.exports = openJsonOutputStream;