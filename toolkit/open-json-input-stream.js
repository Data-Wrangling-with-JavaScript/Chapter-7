'use strict';

const bfj = require('bfj');
const fs = require('fs');
const stream = require('stream');

//
// Open a streaming JSON file for input.
//
function openJsonInputStream (inputFilePath ) {

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
        jsonInputStream.emit("error", err);
    });

    return jsonInputStream;
};

module.exports = openJsonInputStream;