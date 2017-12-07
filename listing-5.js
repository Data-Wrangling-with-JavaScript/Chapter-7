'use strict';

const openCsvInputStream = require('./toolkit/open-csv-input-stream');
const openMongodbOutputStream = require('./toolkit/open-mongodb-output-stream');

openCsvInputStream("./data/weather-stations.csv")
    .pipe(openMongodbOutputStream({
        host: "mongodb://localhost",  // Output database settings.
        database: "weather_stations", 
        collection: "daily_readings"
    }));