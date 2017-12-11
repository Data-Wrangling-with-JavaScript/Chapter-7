'use strict;'

const dataForge = require('data-forge');

const inputFilePath = './data/weather-stations.csv';
const outputFilePath = './output/transformed.csv';

//
// Convert the temperature for a single record.
// Converts from 'tenths of degress celcius' to 'degrees celcius'.
//
// https://earthscience.stackexchange.com/questions/5015/what-is-celsius-degrees-to-tenths
//
function transformRow (inputRow) {

    // Your code here to transform a row of data.

    const outputRow = Object.assign({}, inputRow); // Clone record, prefer not to modify source data.

    if (typeof(outputRow.MinTemp) === 'number') {
        outputRow.MinTemp /= 10;
    }
    else {
        outputRow.MinTemp = undefined;
    }

    if (typeof(outputRow.MaxTemp) === 'number') {
        outputRow.MaxTemp /= 10;
    }
    else {
        outputRow.MaxTemp = undefined;
    }

    return outputRow;
};

dataForge.readFileStream(inputFilePath)
    .parseCSV()
    .parseNumbers(["MinTemp", "MaxTemp"])
    .select(transformRow)
    .asCSV()
    .writeFileStream(outputFilePath)
    .then(() => {
        console.log("Done");
    })
    .catch(err => {
        console.error(err);
    })
