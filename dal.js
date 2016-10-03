var MongoClient = require('mongodb').MongoClient
var mongo = require('mongodb');
var assert = require('assert');

var url = 'mongodb://finance.api.polarflare.com:27017/poc';

var getConnection = function(callback) {
  MongoClient.connect(url, function(err, db) {
    assert.equal(null, err);
    console.log("Connected successfully to server");

    callback(db, function() {
      db.close();
    });
  });
}

var getEquityById = function(id, callback) {
  getConnection(function(db) {
    console.log('inside callback, id = ' + id);
    var collection = db.collection('documents');
    
    var o_id = new mongo.ObjectId(id);
    collection.find({'_id': o_id}).toArray(function(err, docs) {
      assert.equal(err, null);
      console.log("Found the following records");
      console.log(docs);
      callback(docs);
    });
  });
};

module.exports = {
  getEquityById: getEquityById
};