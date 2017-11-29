var crypto = require('crypto');
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

// ********************************************* AUTHENTICATION ********************************************* \\

var auth_collection = 'authTokens';

const hashPassword = async (password) => {
  return crypto.createHash('md5').update(password).digest('hex');
}

// Hash the token then query the Database for a matching, active token
const authenticate = async (authToken) => {
  if (!config.authIsEnabled) {
    console.log('authentication is not enabled, returning true automatically');
    return true;
  }

  let authenticated = false;
  let hashedToken = await hashPassword(authToken);
  
  query = {'token': hashedToken};

  docs = await mdb.collection(auth_collection).find(query).toArray();
  for (let doc of docs) {
    if (doc.isActive) {
      authenticated = true;
    }
  }

  return authenticated;
};

const createAuthToken = async (authToken) => {
  let hashedToken = await hashPassword(authToken);

  // Create a random id for the auth token
  authTokenId = randomString(optionsForId);

  // Create the actual token object
  authTokenJson = {
    'id': authTokenId,
    'token': hashedToken,
    'role': 'admin',
    'isActive': true
  }
  let doc = await mdb.collection(auth_collection).insert(authTokenJson);
  
  return doc;
};

// ********************************************* EQUITIES ********************************************* \\

const createEquity = async (equity) => {
  if (equity.id === undefined) {
    equity.id = randomString(optionsForId);
  }
  logger.log('Creating equity');

  let doc = await mdb.collection(equity_collection).insert(equity);
  delete doc._id;
  
  return doc;
};

const getAllEquities = async () => {
  var query = {};
  logger.log('Retrieving all equities');

  docs = mdb.collection(equity_collection).find(query).toArray();
  for (doc in docs) {
    delete docs[doc]._id;
  }
  return docs;
};

// Should this really be allowed?  Seems very exploitable if not controlled properly
const getEquitiesByFilter = async (queryField, queryValue) => {
  var query = {};
  query[queryField] = queryValue;
  logger.log('Retrieving object by ' + JSON.stringify(query));

  docs = await mdb.collection(equity_collection).find(query).toArray();
  for (doc in docs) {
    delete docs[doc]._id;
  }
  
  return docs;
};

const getEquityById = async(id) => {
  var query = null;

  if (id.length > 15) {
    query = {'id': id};
    logger.log('Retrieving object by data store id ' + JSON.stringify(query));
  } else {
    query = {'ticker': id};
    logger.log('Retrieving object by ticker ' + JSON.stringify(query));
  }

  // Get the equities that match this criteria
  let docs = await mdb.collection(equity_collection).find(query).toArray();
  
  // Get the first doc (should only be one) and clear out the database id before passing back.
  var doc = docs.length > 0 ? docs[0] : null;
  logger.log("Retrieved the following records from the database");
  logger.log(doc == null ? 'None found' : doc);
  if (doc != null) {
    delete doc._id;
  }
  
  return doc;
};

const deleteEquity = async (id) => {
  mdb.collection(equity_collection).deleteOne({'id':id});
  logger.log('Equity by id ' + id + ' has been deleted.');
}

var updateEquity = function(id, params, callback) {
  mdb.collection(equity_collection).updateOne({'id':id}, { $set: params }, function(err, result) {
    logger.log('Equity by id ' + id + ' has been updated with ' + JSON.stringify(params));
    callback();
  });
};

// ********************************************* SNAPSHOTS ********************************************* \\

// Creates a new snapshot in the database
const createSnapshot = async (snapshot) => {
  // Create an id for the snapshot
  snapshot.id = randomString(optionsForId);
  
  // Persist the snapshot to the database - removing the database id from the result
  let insertResults = await mdb.collection(snapshot_collection).insert(snapshot);
  let doc = insertResults.ops[0];
  delete doc._id;
  
  return doc;
};


// Retrieves a single snapshot by id
const getSnapshotById = async (id) => {
  var query = {'id': id};
  
  let docs = await mdb.collection(snapshot_collection).find(query).toArray();
  console.log('Docs = ' + JSON.stringify(docs));
  let doc = docs[0];
  delete doc._id;
  
  return doc;
};


// Retrieve snapshots that belong to an individual entity
const getSnapshotsByEquity = async (id, limit) => {
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

  let docs = await mdb.collection(snapshot_collection).find(query, options).toArray();
  for (doc in docs) {
    delete docs[doc]._id;
  }

  return docs;
};


// Deletes an individual snapshot by id
const deleteSnapshotById = async (id) => {
  await mdb.collection(snapshot_collection).deleteOne({'id':id});
  logger.log('Snapshot by id ' + id + ' has been deleted.');
};

// ********************************************* AGGREGATES ********************************************* \\

// Creates a new aggregate in the database
const createAggregate = async (aggregate) => {
  // Assign a random id for the new aggregate
  aggregate.id = randomString(optionsForId);

  // Create the aggregate, return a version stripped of the database id
  let result = await mdb.collection(aggregate_collection).insert(aggregate);
  let createdAggregate = result.ops[0];
  logger.log(JSON.stringify(result.result));
  //logger.log(JSON.stringify(result.ops[0]));
  delete createdAggregate._id;
  
  return createdAggregate;
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

  // Authentication
  authenticate: authenticate,
  createAuthToken: createAuthToken,
  
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
