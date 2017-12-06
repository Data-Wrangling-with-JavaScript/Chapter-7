'use require';

//
// A simple example of Node.js file streaming.
//

const fs = require('fs');

const fileInputStream = fs.createReadStream('./data/weather-stations.csv'); // Create stream for reading the input file.
const fileOutputStream = fs.createWriteStream('./output/streamed-output-file.csv'); // Create stream for writing the output file.

fileInputStream.pipe(fileOutputStream);