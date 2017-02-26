var MongoClient = require('mongodb').MongoClient
//var mongo = require('mongodb');
var assert = require('assert');
var randomString = require('random-string');

var optionsForId = {
  length: 32,
  numeric: true,
  letters: true,
  special: false
};

//var url = 'mongodb://finance.api.polarflare.com:27017/poc';
var url = 'mongodb://localhost:27017/poc'
var equity_collection = 'equities';
var snapshot_collection = 'snapshots';
var aggregate_collection = 'aggregates';

var getConnection = function(callback) {
  MongoClient.connect(url, function(err, db) {
    assert.equal(null, err);
    console.log("Connected successfully to server");

    callback(db, function() {
      db.close();
    });
  });
};

var createEquity = function(equity, callback) {
  equity.id = randomString(optionsForId);
  
  getConnection(function(db) {
    console.log('Creating equity');

    var collection = db.collection(equity_collection);
    collection.insert(equity, function(err, result) {
      
      console.log(JSON.stringify(result.result));
      console.log(JSON.stringify(result.ops));
      delete equity._id;
      callback(equity);
    });
  });
};

var getAllEquities = function(callback) {
  getConnection(function(db) {
    var query = {}
    console.log('Retrieving all equities');
    
    var collection = db.collection(equity_collection);
    collection.find(query).toArray(function(err, docs) {
      for (doc in docs) {
        delete docs[doc]._id;
      }
      callback(docs);
    });
  });           
};

// Should this really be allowed?  Seems very exploitable if not controlled properly
var getEquitiesByFilter = function(queryField, queryValue, callback) {
  getConnection(function(db) {
    var query = {};
    query[queryField] = queryValue;
    console.log('Retrieving object by ' + JSON.stringify(query));
    
    var collection = db.collection(equity_collection);
    collection.find(query).toArray(function(err, docs) {
      for (doc in docs) {
        delete docs[doc]._id;
      }
      callback(docs);
    });
  });
};

var getEquityById = function(id, callback) {
  getConnection(function(db) {
    var query = null;
    
    // Allow queries by both the unique id of the record and the ticker symbol
    if (id.length == 32) {
      query = {'id': id};
    } else {
      query = {'ticker': id.toUpperCase()};
    }
    console.log('Retrieving object by ' + JSON.stringify(query));
    
    var collection = db.collection(equity_collection);
    collection.find(query).toArray(function(err, docs) {
      var doc = docs.length > 0 ? docs[0] : null;
      console.log("Retrieved the following records from the database");
      console.log(doc == null ? 'None found' : doc);
      if (doc != null) {
        delete doc._id;
      }
      callback(doc);
    });
  });
};

var deleteEquity = function(id, callback) {
  getConnection(function(db) {
    var collection = db.collection(equity_collection);
    collection.deleteOne({'id':id}, function(err, result) {
      console.log('Equity by id ' + id + ' has been deleted.');
      callback();
    });
  });
};

var updateEquity = function(id, params, callback) {
  getConnection(function(db) {
    var collection = db.collection(equity_collection);
    collection.updateOne({'id':id}, { $set: params }, function(err, result) {
      console.log('Equity by id ' + id + ' has been updated with ' + JSON.stringify(params));
      callback();
    });
  });
};

// Creates a new snapshot in the database
var createSnapshot = function(snapshot, callback) {
  snapshot.id = randomString(optionsForId);
  
  getConnection(function(db) {
    var collection = db.collection(snapshot_collection);
    collection.insert(snapshot, function(err, result) {
      
      console.log(JSON.stringify(result.result));
      console.log(JSON.stringify(result.ops));
      delete snapshot._id;
      callback(snapshot);
    });
  });
};

// Retrieves a single snapshot by id
var getSnapshotById = function(id, callback) {
  getConnection(function(db) {
    var query = {'id': id};
    console.log('Retrieving object by ' + JSON.stringify(query));
    
    var collection = db.collection(snapshot_collection);
    collection.find(query).toArray(function(err, docs) {
      var doc = docs.length > 0 ? docs[0] : null;
      console.log("Retrieved the following records from the database");
      console.log(doc == null ? 'None found' : doc);
      if (doc != null) {
        delete doc._id;
      }
      callback(doc);
    });
  });
};

var getSnapshotsByTicker = function(ticker, limit, callback) {
  getConnection(function(db) {
    var query = {'ticker': ticker};
    var options = {
      'limit': limit,
      'sort': [['date', 'desc']]
    };
    console.log('Retrieving snapshots by ticker ' + ticker + ' the actual query is ' + JSON.stringify(query) + ' and the query options are ' + JSON.stringify(options));
    
    var collection = db.collection(snapshot_collection);
    collection.find(query, options).toArray(function(err, docs) {
      for (doc in docs) {
        delete docs[doc]._id;
      }
      callback(docs);
    });
  });
};

var deleteSnapshotById = function(id, callback) {
  getConnection(function(db) {
    var collection = db.collection(snapshot_collection);
    collection.deleteOne({'id':id}, function(err, result) {
      console.log('Snapshot by id ' + id + ' has been deleted.');
      callback();
    });
  });
};

// Creates a new aggregate in the database
var createAggregate = function(aggregate, callback) {
  aggregate.id = randomString(optionsForId);
  
  getConnection(function(db) {
    var collection = db.collection(aggregate_collection);
    collection.insert(aggregate, function(err, result) {
      
      console.log(JSON.stringify(result.result));
      console.log(JSON.stringify(result.ops));
      delete aggregate._id;
      callback(aggregate);
    });
  });
};

// Retrieves aggregates by ticker symbol - always sorting by date descending first
var getAggregatesByTicker = function(ticker, limit, callback) {
  getConnection(function(db) {
    var query = {'ticker': ticker};
    var options = {
      'limit': limit,
      'sort': [['date', 'desc']]
    };
    console.log('Retrieving aggregates by ticker ' + ticker + ' the actual query is ' + JSON.stringify(query) + ' and the query options are ' + JSON.stringify(options));
    
    var collection = db.collection(aggregate_collection);
    collection.find(query, options).toArray(function(err, docs) {
      for (doc in docs) {
        delete docs[doc]._id;
      }
      callback(docs);
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
  getSnapshotsByTicker: getSnapshotsByTicker,
  deleteSnapshotById: deleteSnapshotById,
  
  // Aggregates
  createAggregate: createAggregate,
  getAggregatesByTicker: getAggregatesByTicker
};