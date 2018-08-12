'use require';

//
// An example of Node.js file streaming with a transformation.
//

const fs = require('fs');
const stream = require('stream');

const inputFilePath = "./data/weather-stations.csv";
const outputFilePath = "./output/weather-stations-transformed.csv";

const fileInputStream = fs.createReadStream(inputFilePath); // Create stream for reading the input file.
const fileOutputStream = fs.createWriteStream(outputFilePath); // Create stream for writing the output file.

//
// Create a stream that transforms chunks of data as they are incrementally processed.
//
function transformStream () {
    const transformStream = new stream.Transform();
    transformStream._transform = (inputChunk, encoding, callback) => { // Callback to execute on chunks that are input.
        const transformedChunk = inputChunk.toString().toLowerCase(); // Transform the chunk.
        transformStream.push(transformedChunk); // Pass the converted chunk to the output stream.
        callback();
    };
    return transformStream;
};

fileInputStream
    .pipe(transformStream()) // Pipe the file stream through a transformation.
    .pipe(fileOutputStream)
    .on('error', err => {
        console.error(err);
    });