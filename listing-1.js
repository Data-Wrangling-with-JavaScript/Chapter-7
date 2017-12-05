'use strict';

const papaparse = require('papaparse');
const fs = require('fs');

var transformRow = inputRow => {
    // Your code here to transform a record.
    return inputRow;
}

var transformData = inputData => {
    // Your code here to transform a batch of records.
    return inputData.map(inputRow);
};

var csvHeaders = [ "StationId", "Year", "Month", "Day", "Element", "Value", "Mflag", "Qflag", "Sflag" ];

var inputStream = fs.createReadStream('./data/weather-stations.csv');

let outputStream = fs.createWriteStream('./output/transformed-csv.csv');
outputStream.write(csvHeaders.join(',') + '\n');

papaparse.parse(inputStream, {
        header: csvHeaders,
        dynamicTyping: true,

        // We may not need this, but don't want to get halfway through the massive file before realising it is needed.
        skipEmptyLines: true, 

        step: (results) => {
            var outputCSV = papaparse.unparse(results.data, { 
                header: false 
            });
            outputStream.write(outputCSV + '\n');
        },

        complete: () => {
            outputStream.end();
        },

        error: (err) => {
            console.error("An error occurred transform the CSV file.");
            console.error(err);
        }
    });

