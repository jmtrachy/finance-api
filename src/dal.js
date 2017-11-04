var MongoClient = require('mongodb').MongoClient
var assert = require('assert');
var randomString = require('random-string');
var config = require('./config/config.js');
var logger = require('./logger.js');

logger.log('db location is ' + config.env.databaseLocation);

var optionsForId = {
  length: 32,
  numeric: true,
  letters: true,
  special: false
};

//var url = 'mongodb://finance.api.polarflare.com:27017/poc';
var url = 'mongodb://' + config.env.databaseLocation + ':27017/poc'
var equity_collection = 'equities';
var snapshot_collection = 'snapshots';
var aggregate_collection = 'aggregates';

var mdb = null;
var initializeDatabases = function(callback) {
  MongoClient.connect(url, function(err, db) {
    if (err) {
      logger.log('err = ' + err);
    }
    assert.equal(null, err);

    console.log('Connection to mongo established');
    mdb = db;
    callback();
  });
}

// ********************************************* EQUITIES ********************************************* \\

var createEquity = function(equity, callback) {
  if (equity.id === undefined) {
    equity.id = randomString(optionsForId);
  }
  logger.log('Creating equity');

  mdb.collection(equity_collection).insert(equity, function(err, result) {
    logger.log(JSON.stringify(result.result));
    logger.log(JSON.stringify(result.ops));
    delete equity._id;
    callback(equity);
  });
};

var getAllEquities = function(callback) {
  var query = {}
  logger.log('Retrieving all equities');

  mdb.collection(equity_collection).find(query).toArray(function(err, docs) {
    for (doc in docs) {
      delete docs[doc]._id;
    }
    callback(docs);
  });
};

// Should this really be allowed?  Seems very exploitable if not controlled properly
var getEquitiesByFilter = function(queryField, queryValue, callback) {
  var query = {};
  query[queryField] = queryValue;
  logger.log('Retrieving object by ' + JSON.stringify(query));

  mdb.collection(equity_collection).find(query).toArray(function(err, docs) {
    for (doc in docs) {
      delete docs[doc]._id;
    }
    callback(docs);
  });
};

var getEquityById = function(id, callback) {
  var query = null;

  if (id.length > 15) {
    query = {'id': id};
    logger.log('Retrieving object by data store id ' + JSON.stringify(query));
  } else {
    query = {'ticker': id};
    logger.log('Retrieving object by ticker ' + JSON.stringify(query));
  }

  mdb.collection(equity_collection).find(query).toArray(function(err, docs) {
    var doc = docs.length > 0 ? docs[0] : null;
    logger.log("Retrieved the following records from the database");
    logger.log(doc == null ? 'None found' : doc);
    if (doc != null) {
      delete doc._id;
    }
    callback(doc);
  });
};

var deleteEquity = function(id, callback) {
    mdb.collection(equity_collection).deleteOne({'id':id}, function(err, result) {
      logger.log('Equity by id ' + id + ' has been deleted.');
      callback();
    });
};

var updateEquity = function(id, params, callback) {
  mdb.collection(equity_collection).updateOne({'id':id}, { $set: params }, function(err, result) {
    logger.log('Equity by id ' + id + ' has been updated with ' + JSON.stringify(params));
    callback();
  });
};

// ********************************************* SNAPSHOTS ********************************************* \\

// Creates a new snapshot in the database
var createSnapshot = function(snapshot, callback) {
  // Create an id for the snapshot
  snapshot.id = randomString(optionsForId);
  
  // Persist the snapshot to the database - removing the database id from the result
  mdb.collection(snapshot_collection).insert(snapshot, function(err, result) {
    logger.log(JSON.stringify(result.result));
    logger.log(JSON.stringify(result.ops));
    delete snapshot._id;
    callback(snapshot);
  });
};


// Retrieves a single snapshot by id
var getSnapshotById = function(id, callback) {
  var query = {'id': id};
  mdb.collection(snapshot_collection).find(query).toArray(function(err, docs) {
    var doc = docs.length > 0 ? docs[0] : null;
    logger.log("Retrieved the following records from the database");
    logger.log(doc == null ? 'None found' : doc);
    if (doc != null) {
      delete doc._id;
    }
    callback(doc);
  });
};


// Retrieve snapshots that belong to an individual entity
var getSnapshotsByEquity = function(id, limit, callback) {
  var query = {};

  if (id.length > 15) {
    query = {'equityId': id};
    logger.log('Retrieving object by data store id ' + JSON.stringify(query));
  } else {
    query = {'ticker': id};
    logger.log('Retrieving object by ticker ' + JSON.stringify(query));
  }

  var options = {
    'limit': limit,
    'sort': [['date', 'desc']]
  };

  mdb.collection(snapshot_collection).find(query, options).toArray(function(err, docs) {
    for (doc in docs) {
      delete docs[doc]._id;
    }
    callback(docs);
  });

};


// Deletes an individual snapshot by id
var deleteSnapshotById = function(id, callback) {
  mdb.collection(snapshot_collection).deleteOne({'id':id}, function(err, result) {
    logger.log('Snapshot by id ' + id + ' has been deleted.');
    callback();
  });
};

// ********************************************* AGGREGATES ********************************************* \\

// Creates a new aggregate in the database
var createAggregate = function(aggregate, callback) {
  // Assign a random id for the new aggregate
  aggregate.id = randomString(optionsForId);

  // Create the aggregate, return a version stripped of the database id
  mdb.collection(aggregate_collection).insert(aggregate, function(err, result) {
    logger.log(JSON.stringify(result.result));
    logger.log(JSON.stringify(result.ops));
    delete aggregate._id;
    callback(aggregate);
  });
};

// Retrieves aggregates by ticker symbol - always sorting by date descending first
var getAggregatesByEquity = function(id, limit, callback) {
  var query = {};
  if (id.length > 15) {
    query = {'equityId': id};
    logger.log('Retrieving object by data store id ' + JSON.stringify(query));
  } else {
    query = {'ticker': id.toUpperCase()};
    logger.log('Retrieving object by ticker ' + JSON.stringify(query));
  }

  var options = {
    'limit': limit,
    'sort': [['date', 'desc']]
  };

  logger.log('Retrieving aggregates by ticker ' + id + ' the actual query is ' + JSON.stringify(query) + ' and the query options are ' + JSON.stringify(options));

  mdb.collection(aggregate_collection).find(query, options).toArray(function(err, docs) {
    for (doc in docs) {
      delete docs[doc]._id;
    }
    callback(docs);
  });
};

// Retrieves a single aggregate by id
var getAggregateById = function(id, callback) {
  var query = {'id': id};
  logger.log('Retrieving object by ' + JSON.stringify(query));

  mdb.collection(aggregate_collection).find(query).toArray(function(err, docs) {
    var doc = docs.length > 0 ? docs[0] : null;
    logger.log("Retrieved the following records from the database");
    logger.log(doc == null ? 'None found' : doc);
    if (doc != null) {
      delete doc._id;
    }
    callback(doc);
  });
};

var deleteAggregateById = function(id, callback) {
  mdb.collection(aggregate_collection).deleteOne({'id':id}, function(err, result) {
    logger.log('Aggregate by id ' + id + ' has been deleted.');
    callback();
  });
};

module.exports = {
  // Initialize the database
  initializeDatabases: initializeDatabases,
  
  // Equities
  getAllEquities: getAllEquities,
  getEquityById: getEquityById,
  createEquity: createEquity,
  deleteEquity: deleteEquity,
  updateEquity: updateEquity,
  getEquitiesByFilter: getEquitiesByFilter,
  
  // Snapshots
  createSnapshot: createSnapshot,
  getSnapshotById: getSnapshotById,
  getSnapshotsByEquity: getSnapshotsByEquity,
  deleteSnapshotById: deleteSnapshotById,
  
  // Aggregates
  createAggregate: createAggregate,
  getAggregateById: getAggregateById,
  getAggregatesByEquity: getAggregatesByEquity,
  deleteAggregateById: deleteAggregateById
};
