'use strict';

const bfj = require('bfj');
const fs = require('fs');
const stream = require('stream');

let transformRow = inputRow => {
    // Your code here to transform a record.
    return inputRow;
}

let transformData = inputData => {
    // Your code here to transform a batch of records.
    return inputData.map(inputRow);
};

//
// Open a streaming JSON file for input.
//
var openJsonInputStream = inputFilePath => {

    const jsonInputStream = new stream.Readable({ objectMode: true });
    jsonInputStream._read = () => {}; // Must include, otherwise we get an error.

    const fileInputStream = fs.createReadStream(inputFilePath);

    let curObject = null;
    let curProperty = null;

    const emitter = bfj.walk(fileInputStream);

    emitter.on(bfj.events.object, () => {
        curObject = {};
    });

    emitter.on(bfj.events.property, name => {
        curProperty = name;
    });

    let onValue = value => {
        curObject[curProperty] = value;
        curProperty = null;
    };

    emitter.on(bfj.events.string, onValue);
    emitter.on(bfj.events.number, onValue);
    emitter.on(bfj.events.literal, onValue);

    emitter.on(bfj.events.endObject, () => {
        jsonInputStream.push(curObject); // Push results as they are streamed from the file.

        curObject = null; // Finished processing this object.
    });

    emitter.on(bfj.events.endArray, () => {
        jsonInputStream.push(null); // Signify end of stream.
    });

    emitter.on(bfj.events.error, err => {
        jsonInputStream.emit('error', err);
    });

    return jsonInputStream;
};

//
// Open a streaming JSON file for output.
//
var openJsonOutputStream = outputFilePath => {

    let fileOutputStream = fs.createWriteStream(outputFilePath);
    fileOutputStream.write("[\n");

    var numRecords = 0;
    
    let jsonOutputStream = new stream.Writable({ objectMode: true });
    jsonOutputStream._write = (chunk, encoding, callback) => {
        if (numRecords > 0) {
            fileOutputStream.write(",\n");
        }

        // Output a single row of a JSON array.
        // Note: don't include indentation when working a big file.
        // I only include indentation here when testing the code on a small file.
        let jsonData = JSON.stringify(curObject, null, 4);  //TODO: get rid of indentation
        fileOutputStream.write(jsonData + '\n');
        numRecords += chunk.length;
        callback();        
    };
    jsonOutputStream._destroy = () => {
        fileOutputStream.end();
    };

    return jsonOutputStream;
};

//todo: add transform stream

//
// Use Node.js streams to pipe the content of one CSV file to another.
//
openJsonInputStream('./data/weather-stations.json')
    //todo: .pipe(convertTemperatureStream())
    .pipe(openJsonOutputStream('./output/transformed-json.json'))
    .on('error', err => {
        console.error("An error occurred while transforming the JSON file.");
        console.error(err);
    });
