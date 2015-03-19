'use strict';

var Q = require('q');
var _ = require('lodash');
var fs = require('fs');
var moment = require('moment');
var splunk_sdk = require('splunk-sdk');
var Async = splunk_sdk.Async;

function Splunk(log, config) {
    this.log = log;
    this.config = config;
}

Splunk.prototype.connect = function(done) {
    var service = this.service = new splunk_sdk.Service(this.config);
    service.login(function(err) {
        if (err) {
            done(err);
        }
        else {
            done(null, service);
        }
    }); 
};

Splunk.prototype.search = function(query, startDate, endDate, eventName, done) {
    var self = this;
    this.connect(function(err, service) {
        if (err) {
            done(err);
        }
        else {
            var options = {
                output_mode: 'JSON',
                earliest_time: startDate.format(),
                latest_time: endDate.format(),
                max_count: 5000000
            };

            Async.chain([
                // Create a search
                function(done) { service.search(query, options, done); },
                // Poll until the search is complete
                function(job, done) {
                    Async.whilst(
                        function() { return !job.properties().isDone; },
                        function(iterationDone) {
                            job.fetch(function(err, job) {
                                if (err) {
                                    callback(err);
                                }
                                else {
                                    // If the user asked for verbose output,
                                    // then write out the status of the search
                                    var properties = job.properties();
                                    var stats = "-- " +
                                        (properties.doneProgress * 100.0) + "%" + " done | " +
                                        properties.scanCount  + " scanned | " +
                                        properties.eventCount  + " matched | " +
                                        properties.resultCount  + " results | " +
                                        query + " query";
                                    self.log.debug("\r" + stats + "                                          ");

                                    Async.sleep(1000, iterationDone);
                                }
                            });
                        },
                        function(err) {
                            self.log.debug("\r");
                            done(err, job);
                        }
                    );
                },
                function(job, done) {
                    // Run an asynchronous while loop using the Async.whilst helper function to
                    // loop through each set of results 
                    var offset = 0;
                    var count = 10000;
                    var mode = 'row';
                    var allResults = [];

                    // The splunk log contains lots of Foo=bar Bar=foo and we want a map of that
                    var dataScan = /([A-Za-z_]+)=([^,\s\[\]]+)/g;
                    Async.whilst(
                        function() { return offset < job.properties().resultCount; },
                        function(iterationDone) {
                            self.log.debug('Grabbing results, count=' + count + ', offset=' + offset + ', total=' + job.properties().resultCount);
                            job.results({count: count, offset: offset, json_mode: mode }, function(err, results) {
                                var result = _.reduce(results.rows, function(accum, value, key) {
                                    var zipped = _.zipObject(results.fields, value);

                                    // The actual data we want is in the raw string and we need to parse it
                                    var attrs = {}, regexpResults;
                                    while ((regexpResults = dataScan.exec(zipped._raw)) !== null) {
                                        attrs[regexpResults[1]] = regexpResults[2];
                                    }
                                    attrs.Time = moment(zipped._time).unix();
                                    attrs._raw = zipped._raw;
                                    accum.push(attrs);
                                    return accum;
                                }, []);

                                allResults = allResults.concat(result);
                                offset += count;
                                Async.sleep(100, iterationDone);
                            });
                        },
                        function(err) {
                            done(err, allResults, job);
                        }
                    );
                },
                // Print them out (as JSON), and cancel the job
                function(results, job, done) {
                    var fileName = './splunk/' + eventName + '--' + startDate.format('YYYY_MM_DD__HH_mm_ss') + '_' + endDate.format('YYYY_MM_DD__HH_mm_ss') + '.json';
                    self.log.debug('Writing splunk data to ' + fileName);
                    fs.readFile(fileName, function(err, data) {
                        if (err) {
                            fs.writeFile(fileName, JSON.stringify(results), function(err) {
                                if (err) {
                                    self.log.error('Unable to write splunk log, err=' + err);
                                }
                                else {
                                    self.log.debug('Successfully wrote splunk data to ', fileName);
                                }
                                job.cancel(function(err) { done(err, results); });
                            })
                        }
                        else {
                            var jsonArray = JSON.parse(data);
                            jsonArray = jsonArray.concat(results);
                            fs.writeFile(fileName, JSON.stringify(jsonArray), function(err) {
                                if (err) {
                                    self.log.error('Unable to write splunk log, err=' + err);
                                }
                                else {
                                    self.log.debug('Successfully wrote splunk data to ', fileName);
                                }
                                job.cancel(function(err) { done(err, results); });
                            });
                        }
                    });
                }
            ],
            done);
        }
    })
};

module.exports = Splunk;