var mysql = require('mysql');
var q = require('q');
var _ = require('lodash');
var logger = {
  logError: console.error
}
var config = {
  // host: 'localhost',
  // user: 'root',
  // password: 'KW122m3',
  // schema: 'fetchtv_bi',
  charset: 'UTF8_UNICODE_CI'
};

function getConnection() {
  var connection = mysql.createConnection(config);

  connection.connect();
  return connection;
}

function endConnection(connection, onSuccess) {
  var deferred = q.defer();

  connection.end(function(err) {
    if (err) {
      logger.logError(err);
    }
    if (onSuccess) {
      onSuccess();
    }
    deferred.resolve();
  });

  return deferred.promise;
}

function beginTransaction(connection) {
  var deferred = q.defer();

  connection.beginTransaction(function(err) {
    if (err) {
      deferred.reject(err);
    } else {
      deferred.resolve();
    }
  });

  return deferred.promise;
}

function commitTransaction(connection) {
  var deferred = q.defer();

  connection.commit(function(err) {
    if (err) {
      connection.rollback(function() {
        deferred.reject(err);
      });
    }
    deferred.resolve();
  });

  return deferred.promise;
}

function queryWithConnection(connection, queryText, params, forceSuccess) {
  var deferred = q.defer();

  var _query = _.bind(connection.query, connection);

  _query(queryText, params, function(err, result, fields) {
    if (err) {
      console.log('error for query ' + queryText + ' -- ' + JSON.stringify(params));
      console.log('error: ' + err);
      if (forceSuccess) {
        deferred.resolve(null, null);
      } else {
        deferred.reject(err);
      }
    } else {
      deferred.resolve(result, fields);
    }
  });
  return deferred.promise;
}

function query(queryText, params, force) {
  var deferred = q.defer();

  var connection = getConnection();
  var _query = _.bind(connection.query, connection);

  _query(queryText, params, function(err, result, fields) {
    connection.end(function(err2) {
      if (err) {
        console.log('error for query ' + queryText + ' -- ' + JSON.stringify(params));
        console.log('error: ' + err);
        deferred.reject(err);
      } else {
        deferred.resolve(result, fields);
      }
    });
  });
  return deferred.promise;
}

function nonLockingQuery(queryText, params, forceSuccess) {
  var deferred = q.defer();

  var connection = getConnection();
  var _query = _.bind(connection.query, connection);

  _query('SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;', null, function(err1, result1, fields) {
    if (err1) {
      console.log('error: ' + err1);
      deferred.reject(err1);
    } else {
      _query(queryText, params, function(err2, result, fields) {
        if (err2) {
          console.log('error for query ' + queryText + ' -- ' + JSON.stringify(params));
          console.log('error: ' + err2);
          deferred.reject(err2);
        } else {
          _query('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;', null, function(err3, result3, fields) {
            connection.end(function(err4) {
              if (err4) {
                console.log('error for query ' + queryText + ' -- ' + JSON.stringify(params));
                console.log('error: ' + err4);
                deferred.reject(err4);
              } else {
                deferred.resolve(result, fields);
              }
            });
          });
        }
      });
    }
  });

  return deferred.promise;
}

module.exports.getConnection = getConnection;
module.exports.endConnection = endConnection;
module.exports.beginTransaction = beginTransaction;
module.exports.commitTransaction = commitTransaction;
module.exports.queryWithConnection = queryWithConnection;
module.exports.nonLockingQuery = nonLockingQuery;
module.exports.query = query;
module.exports.config = config;
