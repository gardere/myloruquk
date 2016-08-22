var q = require('q');
var mysql = require('./mysql.js');
var _ = require('lodash');

var argv = require('yargs')

    .demand('u')
    .nargs('u', 1)
    .alias('u', 'username')
    .describe('u', 'MySQL username')

    .nargs('h', 1)
    .alias('h', 'host')
    .describe('h', 'mysql host')
    .default('h', 'localhost')

    .nargs('p', 1)
    .default('p', null)
    .alias('p', 'password')
    .describe('p', 'mysql password')

    .default('m', 60)
    .nargs('m', 1)
    .alias('m', 'max-running-time')
    .describe('m', 'Query max running time')

    .argv;


mysql.config.host = argv.h;
mysql.config.password = argv.p;
mysql.config.user = argv.u;


function getLongRunningQueries() {
  console.log('Getting long running queries');
  return mysql.query('SHOW PROCESSLIST')
      .then(function (rows) {
        var rows =  _.filter(rows, function (row) {
          return row.Time >= argv.m && row.Command !== 'Sleep';
        });
        console.log(rows.length + ' slow queries found');
        return rows;
      });
}

function killProcesses(processes) {
  var promises = [];

  _.each(processes, function (process) {
    promises.push(function () {
      console.log('killing query ' +  process.Id + ' (' + process.Info + ')');
      return mysql.query('KILL ' + process.Id);
    }());
  });

  return q.all(promises);
}

function scheduleNextRun() {
  setTimeout(killLongRunningQueries, 10000);
}

function showError(err) {
  console.error(err);
  scheduleNextRun();
}

function killLongRunningQueries() {
  return getLongRunningQueries()
      .then(killProcesses)
      .then(scheduleNextRun)
      .catch(showError);
}


module.exports.killLongRunningQueries = killLongRunningQueries;