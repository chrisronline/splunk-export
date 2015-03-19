'use strict';

var request = require('request');

var MixPanel = {
	send: function(config, data, log, done) {
		var dataStr = new Buffer(JSON.stringify(data)).toString('base64');
		var url = 'http://api.mixpanel.com/import/?data=' + dataStr + '&api_key=' + config.apiKey + '&verbose=1';
		request(url, function(err, response, body) {
	        if (err) {
	            log.error('Error sending match, err=', err);
	        }
	        else if (body === '0') {
	            log.error('Failed to send batch!');
	        }
	        else if (response.statusCode >= 200 && response.statusCode < 300) {
	        	log.debug('Sent batch successfully, body=' + body + ', status=' + response.statusCode);
	        }
	        else {
	        	// Write the url to a file
	            var date = +new Date();
	            fs.writeFile(__dirname + '/mixpanel-failures/' + (date) + '.txt', url);
	            fs.writeFile(__dirname + '/mixpanel-failures/' + (date) + '_data.json', JSON.stringify(data));
	        	log.error('Batch returned non 200 response, body=' + body + ', status=' + response.statusCode);
	        }
	        done();
	    });
	}
};

module.exports = MixPanel;