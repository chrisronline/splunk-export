'use strict';

var tds = require('tedious');

function connectToDatabase(config, done) {
    var connection = new tds.Connection(config);
    connection.on('connect', function(err) {
        done(connection);
    });
};


function query(config, query, done) {
    var results = [];

    connectToDatabase(config, function(connection) {
        var request = new tds.Request(query, function(err) {
            if (err) {
                console.error('Unable to execute request, query=' + query + ', err=' + err);
                done(err);
                return;
            }
            done(null, results);
            connection.close();
        });
        request.on('row', function(columns) {
            var data = {};
            columns.forEach(function(column) {
                data[column.metadata.colName] = column.value;
            });
            results.push(data);
        });
        connection.execSql(request);
    });
}

module.exports = {
    query: query
};