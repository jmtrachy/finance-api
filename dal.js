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
    var collection = db.collection('documents');
    
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
    console.log('Retrieving object by id ' + id);
    var collection = db.collection('documents');
    
    collection.find({'id': id}).toArray(function(err, docs) {
      var doc = docs.length > 0 ? docs[0] : null;
      console.log("Found the following records");
      console.log(doc);
      callback(doc);
    });
  });
};

module.exports = {
  getEquityById: getEquityById,
  createEquity: createEquity
};