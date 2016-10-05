var MongoClient = require('mongodb').MongoClient
var mongo = require('mongodb');
var assert = require('assert');
var randomString = require('random-string');

var optionsForId = {
  length: 32,
  numeric: true,
  letters: true,
  special: false
};

var url = 'mongodb://finance.api.polarflare.com:27017/poc';
var db_collection = 'documents';

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

    var collection = db.collection(db_collection);
    collection.insert(equity, function(err, result) {
      
      console.log(JSON.stringify(result.result));
      console.log(JSON.stringify(result.ops));
      delete equity._id;
      callback(equity);
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
    
    var collection = db.collection(db_collection);
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
    var collection = db.collection(db_collection);
    collection.deleteOne({'id':id}, function(err, result) {
      console.log('Equity by id ' + id + ' has been deleted.');
      callback();
    });
  });
};

var updateEquity = function(id, params, callback) {
  getConnection(function(db) {
    var collection = db.collection(db_collection);
    collection.updateOne({'id':id}, { $set: params }, function(err, result) {
      console.log('Equity by id ' + id + ' has been updated with ' + JSON.stringify(params));
      callback();
    });
  });
};

module.exports = {
  getEquityById: getEquityById,
  createEquity: createEquity,
  deleteEquity: deleteEquity,
  updateEquity: updateEquity
};