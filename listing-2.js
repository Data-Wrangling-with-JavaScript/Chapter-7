'use strict';

const bfj = require('bfj');
const fs = require('fs');

let transformRow = inputRow => {
    // Your code here to transform a record.
    return inputRow;
}

let transformData = inputData => {
    // Your code here to transform a batch of records.
    return inputData.map(inputRow);
};

let inputStream = fs.createReadStream("./data/weather-stations.json");

let outputStream = fs.createWriteStream('./output/transformed-json.json');
outputStream.write("[\n");

let curObject = null;
let curProperty = null;
let numRecords = 0;

const emitter = bfj.walk(inputStream);

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

    if (numRecords > 0) {
        outputStream.write(",\n");
    }

    // Output a single row of a JSON array.
    // Note: don't include indentation when working a big file.
    // I only include indentation here when testing the code on a small file.
    let jsonData = JSON.stringify(curObject, null, 4); 
    outputStream.write(jsonData + '\n');

    curObject = null;
    ++numRecords;
});

emitter.on(bfj.events.endArray, () => {
    outputStream.end();

    console.log("Processed " + numRecords + " records.");
});

emitter.on(bfj.events.error, err => {
    console.error("Error occurred transforming JSON file.");
    console.error(err);
});

