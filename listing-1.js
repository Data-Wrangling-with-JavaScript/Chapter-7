'use require';

//
// A simple example of Node.js file streaming.
//

const fs = require('fs');

const inputFile = './data/weather-stations.csv';
const outputFilePath = './output/streamed-output-file.csv';

const fileInputStream = fs.createReadStream(inputFilePath); // Create stream for reading the input file.
const fileOutputStream = fs.createWriteStream(outputFilePath); // Create stream for writing the output file.

fileInputStream.pipe(fileOutputStream);