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

var getConnection = function(callback) {
  MongoClient.connect(url, function(err, db) {
    if (err) {
      logger.log('err = ' + err);
    }
    assert.equal(null, err);
    logger.log("Connected successfully to mongo");

    callback(db, function() {
      logger.log("About to close the connection.");
      db.close();
    });
  });
};

var createEquity = function(equity, callback) {
  if (equity.id === undefined) {
    equity.id = randomString(optionsForId);
  }
  
  getConnection(function(db, close_connection) {
    logger.log('Creating equity');

    var collection = db.collection(equity_collection);
    collection.insert(equity, function(err, result) {
      
      logger.log(JSON.stringify(result.result));
      logger.log(JSON.stringify(result.ops));
      delete equity._id;
      callback(equity);
      close_connection();
    });
  });
};

var getAllEquities = function(callback) {
  getConnection(function(db, close_connection) {
    var query = {}
    logger.log('Retrieving all equities');
    
    var collection = db.collection(equity_collection);
    collection.find(query).toArray(function(err, docs) {
      for (doc in docs) {
        delete docs[doc]._id;
      }
      callback(docs);
      close_connection();
    });
  });           
};

// Should this really be allowed?  Seems very exploitable if not controlled properly
var getEquitiesByFilter = function(queryField, queryValue, callback) {
  getConnection(function(db, close_connection) {
    var query = {};
    query[queryField] = queryValue;
    logger.log('Retrieving object by ' + JSON.stringify(query));
    
    var collection = db.collection(equity_collection);
    collection.find(query).toArray(function(err, docs) {
      for (doc in docs) {
        delete docs[doc]._id;
      }
      callback(docs);
      close_connection();
    });
  });
};

var getEquityById = function(id, callback) {
  getConnection(function(db, close_connection) {
    var query = null;
    
    if (id.length > 15) {
      query = {'id': id};
      logger.log('Retrieving object by data store id ' + JSON.stringify(query));
    } else {
      query = {'ticker': id};
      logger.log('Retrieving object by ticker ' + JSON.stringify(query));
    }
    
    var collection = db.collection(equity_collection);
    collection.find(query).toArray(function(err, docs) {
      var doc = docs.length > 0 ? docs[0] : null;
      logger.log("Retrieved the following records from the database");
      logger.log(doc == null ? 'None found' : doc);
      if (doc != null) {
        delete doc._id;
      }
      callback(doc);
      close_connection();
    });
  });
};

var deleteEquity = function(id, callback) {
  getConnection(function(db, close_connection) {
    var collection = db.collection(equity_collection);
    collection.deleteOne({'id':id}, function(err, result) {
      logger.log('Equity by id ' + id + ' has been deleted.');
      callback();
      close_connection();
    });
  });
};

var updateEquity = function(id, params, callback) {
  getConnection(function(db, close_connection) {
    var collection = db.collection(equity_collection);
    collection.updateOne({'id':id}, { $set: params }, function(err, result) {
      logger.log('Equity by id ' + id + ' has been updated with ' + JSON.stringify(params));
      callback();
      close_connection();
    });
  });
};

// Creates a new snapshot in the database
var createSnapshot = function(snapshot, callback) {
  snapshot.id = randomString(optionsForId);
  
  getConnection(function(db, close_connection) {
    var collection = db.collection(snapshot_collection);
    collection.insert(snapshot, function(err, result) {
      close_connection();
      
      logger.log(JSON.stringify(result.result));
      logger.log(JSON.stringify(result.ops));
      delete snapshot._id;
      callback(snapshot);
    });
  });
};

// Retrieves a single snapshot by id
var getSnapshotById = function(id, callback) {
  getConnection(function(db, close_connection) {
    var query = {'id': id};
    logger.log('Retrieving object by ' + JSON.stringify(query));
    
    var collection = db.collection(snapshot_collection);
    collection.find(query).toArray(function(err, docs) {
      var doc = docs.length > 0 ? docs[0] : null;
      logger.log("Retrieved the following records from the database");
      logger.log(doc == null ? 'None found' : doc);
      if (doc != null) {
        delete doc._id;
      }
      callback(doc);
      close_connection();
    });
  });
};

var getSnapshotsByEquity = function(id, limit, callback) {
  getConnection(function(db, close_connection) {
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
    
    var collection = db.collection(snapshot_collection);
    collection.find(query, options).toArray(function(err, docs) {
      for (doc in docs) {
        delete docs[doc]._id;
      }
      callback(docs);
      close_connection();
    });
  });
};

var deleteSnapshotById = function(id, callback) {
  getConnection(function(db, close_connection) {
    var collection = db.collection(snapshot_collection);
    collection.deleteOne({'id':id}, function(err, result) {
      logger.log('Snapshot by id ' + id + ' has been deleted.');
      callback();
      close_connection();
    });
  });
};

// Creates a new aggregate in the database
var createAggregate = function(aggregate, callback) {
  aggregate.id = randomString(optionsForId);
  
  getConnection(function(db, close_connection) {
    var collection = db.collection(aggregate_collection);
    collection.insert(aggregate, function(err, result) {
      
      logger.log(JSON.stringify(result.result));
      logger.log(JSON.stringify(result.ops));
      delete aggregate._id;
      callback(aggregate);
      close_connection();
    });
  });
};

// Retrieves aggregates by ticker symbol - always sorting by date descending first
var getAggregatesByEquity = function(id, limit, callback) {
  getConnection(function(db, close_connection) {
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
    
    var collection = db.collection(aggregate_collection);
    collection.find(query, options).toArray(function(err, docs) {
      for (doc in docs) {
        delete docs[doc]._id;
      }
      callback(docs);
      close_connection();
    });
  });
};

// Retrieves a single aggregate by id
var getAggregateById = function(id, callback) {
  getConnection(function(db, close_connection) {
    var query = {'id': id};
    logger.log('Retrieving object by ' + JSON.stringify(query));
    
    var collection = db.collection(aggregate_collection);
    collection.find(query).toArray(function(err, docs) {
      var doc = docs.length > 0 ? docs[0] : null;
      logger.log("Retrieved the following records from the database");
      logger.log(doc == null ? 'None found' : doc);
      if (doc != null) {
        delete doc._id;
      }
      callback(doc);
      close_connection();
    });
  });
};

var deleteAggregateById = function(id, callback) {
  getConnection(function(db, close_connection) {
    var collection = db.collection(aggregate_collection);
    collection.deleteOne({'id':id}, function(err, result) {
      logger.log('Aggregate by id ' + id + ' has been deleted.');
      callback();
      close_connection();
    });
  });
};

module.exports = {
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
