'use strict';

var async = require('async');
var fs = require('fs');
var _ = require('lodash');
var winston = require('winston');
var moment = require('moment'); require('moment-range');
var DB = require('./db');
var SplunkCls = require('./splunk');
var MassageData = require('./massage');
var Mixpanel = require('./mixpanel');

// Ensure output dirs exist
try { fs.mkdirSync('./import/logs'); } catch (e) { }
try { fs.mkdirSync('./import/splunk'); } catch (e) { }
try { fs.mkdirSync('./import/mixpanel-failures'); } catch (e) { }

var config = JSON.parse(fs.readFileSync(__dirname + '/config.json'));
var dateRange = config.search.range;
var batchSize = config.mixpanel.batchSize;
var startDate = moment(config.search.start);
var endDate = moment(config.search.end);
var dates = [];
moment()
	.range(startDate, endDate)
	.by(dateRange + 's', function(date) {
		dates.push({ start: date, end: date.clone().add(1, dateRange) });
	});

async.eachSeries(dates, function(dateRange, dateDone) {
	async.eachSeries(config.search.searches, function(search, searchDone) {
		console.log('Start processing ' + search.eventName + ' for ' + dateRange.start.format() + ' - ' + dateRange.end.format());
		var log = new (winston.Logger)({
			transports: [
				new (winston.transports.File)({
					filename: __dirname + '/logs/' + search.eventName + '--' + dateRange.start.format('YYYY_MM_DD__HH_mm_ss') + '_' + dateRange.end.format('YYYY_MM_DD__HH_mm_ss') + '.log',
					level: 'debug'
				})
			]
		});
		var splunk = new SplunkCls(log, config.splunk);
		splunk.search(search.query, dateRange.start, dateRange.end, search.eventName, function(err, results) {
			if (err) {
			    log.error('Error getting data from splunk, err=' + err);
			    searchDone(err);
			    return;
			}

            MassageData(config, search.eventName, results, log, function(err, data) {
                var chunked = _.chunk(data, batchSize);
                var requestsTotal = chunked.length;
                var requestsCompleted = 0;

                log.debug('Data massaged...sending to MixPanel...', chunked.length);

                if (chunked.length) {
                    _.each(chunked, function(chunk) {
                        Mixpanel.send(config.mixpanel, chunk, log, function(err) {
                            log.debug('Progress=' + (++requestsCompleted) + '/' + requestsTotal);
                            if (requestsCompleted >= requestsTotal) {
                                log.debug('Done processing ' + search.eventName + ' for ' + dateRange.start.format() + ' - ' + dateRange.end.format())
                                console.log('Done processing ' + search.eventName + ' for ' + dateRange.start.format() + ' - ' + dateRange.end.format());
                                searchDone(null);
                            }
                        });
                    });
                }
                else {
                    log.debug('Done processing ' + search.eventName + ' for ' + dateRange.start.format() + ' - ' + dateRange.end.format())
                    console.log('Done processing ' + search.eventName + ' for ' + dateRange.start.format() + ' - ' + dateRange.end.format());
                    searchDone(null);
                }
            });
		});
	}, dateDone);
}, function(err) {
	if (err) {
		console.log('Error running import, err=' + err);
		return;
	}
	console.log('All done!');
});