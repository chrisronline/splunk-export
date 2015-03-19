'use strict';

var request = require('request');
var _ = require('lodash');
var moment = require('moment');
var db = require('./db');

function getByTeam(config, teamIds, done) {
	if (!teamIds.length) {
		done(null, []);
		return;
	}

	db.query(config.sql, config.sql.queries.getByTeam.replace('{teamIds}', teamIds.join(',')), done);
}

function getByUser(config, userIds, done) {
	if (!userIds.length) {
		done(null, []);
		return;
	}

	db.query(config.sql, config.sql.queries.getByUser.replace('{userIds}', userIds.join(',')), done);
}

function findMatching(collection, unixTime) {
	return _.last(
		_.filter(collection, function(row) {
			return unixTime.isBetween(row.datesent, row.subscriptionenddate);
		}
	));
}

function Massage(config, eventName, results, log, done) {

	// Its either Team or AuthUser
	var teamIds = _.uniq(_.compact(_.pluck(results, 'Team')));
	var userIds = _.uniq(_.compact(_.pluck(results, 'AuthUser')));

	getByTeam(config, teamIds, function(err, teamRows) {
		if (err) {
			log.error('Error executing team id query..' + err);
			done(err);
			return;
		}

		var teamsGrouped = _.groupBy(teamRows, 'teamid');

		getByUser(config, userIds, function(err, userRows) {
			if (err) {
				log.error('Error executing user id query..' + err);
				done(err);
				return;
			}

			var usersGrouped = _.groupBy(userRows, 'userid');

			var massaged = _(results)
				.map(function(result) {
					var unixTime = moment.unix(result.Time);
					var matchingRow = findMatching(teamsGrouped[result.Team], unixTime) || findMatching(usersGrouped[result.AuthUser], unixTime);
					if (!matchingRow) {
						if (teamsGrouped[result.Team]) {
							log.warn('Team found but no invoices in the right period, team=' + result.Team + ', _raw=' + result._raw);
						}
						else if (usersGrouped[result.AuthUser]) {
							log.warn('User found but no invoices in the right period, user=' + result.AuthUser + ', _raw=' + result._raw);
						}
						else {
							log.warn('Nothing found, team=' + result.Team + ', user=' + result.AuthUser + ', _raw=' + result._raw);
						}
						return null;
					}

					var obj = { event: eventName };

					obj.properties = _.extend(
						{
						    distinct_id: result.AuthUser && result.AuthUser !== '0' ? result.AuthUser : 'Team_' + result.Team,
						    time: result.Time,
						    token: config.mixpanel.token,
						    ip: result.Ip 
						},
						_.omit(result, ['AuthUser', 'Time', 'Ip', '_raw', 'meta']),
						{
							School: matchingRow.schoolid,
							'Account Status': matchingRow.accountstatus,
							'Team Level': matchingRow.teamlevel,
							Trial: matchingRow.istrial,
							'Package Level': matchingRow.package,
							'Date Team Created': matchingRow.datecreated,
							'Date Paid': matchingRow.datepaid
						}
					);
					return obj;
				})
				.compact()
				.value();

			done(null, massaged);
		});
	});
}

module.exports = Massage;