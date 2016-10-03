var MongoClient = require('mongodb').MongoClient
var mongo = require('mongodb');
var assert = require('assert');

var url = 'mongodb://finance.api.polarflare.com:27017/poc';

var insertDocuments = function(db, callback) {
  // Get the documents collection
  var collection = db.collection('documents');
  // Insert some documents
  collection.insertMany([
    {a : 1}//, {a : 2}, {a : 3}
  ], function(err, result) {
    /*assert.equal(err, null);
    assert.equal(3, result.result.n);
    assert.equal(3, result.ops.length);
    console.log(JSON.stringify(result.result));
    console.log(JSON.stringify(result.ops));
    console.log("Inserted 3 documents into the collection");*/
    callback(result);
  });
};

var findDocuments = function(db, callback) {
  // Get the documents collection
  var collection = db.collection('documents');
  // Find some documents
  var o_id = new mongo.ObjectId('57f1dc5fa18d1c090530c768');
  collection.find({'_id': o_id}).toArray(function(err, docs) {
    assert.equal(err, null);
    console.log("Found the following records");
    console.log(docs);
    callback(docs);
  });
};

var updateDocument = function(db, callback) {
  // Get the documents collection
  var collection = db.collection('documents');
  // Update document where a is 2, set b equal to 1
  collection.updateOne({ a : 2 }
    , { $set: { b : 1 } }, function(err, result) {
    assert.equal(err, null);
    assert.equal(1, result.result.n);
    console.log("Updated the document with the field a equal to 2");
    callback(result);
  });  
};

MongoClient.connect(url, function(err, db) {
  assert.equal(null, err);
  console.log("Connected successfully to server");

  /*insertDocuments(db, function() {
    db.close();
  });*/
  
  findDocuments(db, function() {
    db.close();
  });
});