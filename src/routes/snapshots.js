var express = require('express');
var snapshotRouter = express.Router();
var dal = require('../dal.js');

// Set the content type here since it'll always be JS - also just trying to keep code around for having a specific use on a router :)
snapshotRouter.use(function(req, res, next) {
  res.setHeader('Content-Type', 'application/json');
  next();
});

/*
snapshotRouter.get('/', function(req, res) {
  var filter = req.query.filter;
  if (!filter) {
    res.status(400).send(JSON.stringify({error:"'filter' is a required query parameter."}));
  } else {
    filter = filter.toLowerCase();
    if (filter != 'date') {
      res.status(400).send(JSON.stringify({error:'The only options available for the filter parameter are: dow'}));
    } else {
      dal.getEquitiesByFilter('dow', 'True', function(docs) {
        res.status(200).send(JSON.stringify(docs));
      });
    }
  } 
});
*/

// Route for creating an equity
snapshotRouter.post('/', function(req, res) {
  var reqBody = req.body;
  console.log('Post called with ' + JSON.stringify(reqBody));
  
  if (!reqBody.id) {
    dal.createSnapshot(reqBody, function(snapshot) {
      res.end(JSON.stringify(snapshot));
    });
  } else {
    res.status(400).send('{ "error": "Update not supported yet" }');
  }  
});

// Route for retrieving an equity by Id
snapshotRouter.get('/:id', function(req, res) {
  dal.getSnapshotById(req.params.id, function(doc){
    
    if (doc != null) {
      res.status(200).send(JSON.stringify(doc));  
    } else {
      var errorMessage = {
        error: 'No snapshot found with id ' + req.params.id
      }
      res.status(404).send(errorMessage);
    }
    
  });
});

// Deletes a particular record by id
snapshotRouter.delete('/:id', function(req, res) {
  var responseCode = null;
  var id = req.params.id;
  
  // TODO: Add check for 32 characters once db is cleaned up
  if (id == null) {
    responseMessage = { "error": "An id must be passed in to identify the individual resource."};
    res.status(400).send(JSON.stringify(responseMessage));
  } else {
    dal.getSnapshotById(id, function(doc) {
      if (doc == null) {
        res.status(404).send(JSON.stringify({ "error": "No resource found."}));
      } else {
        dal.deleteSnapshotById(id, function() {
          res.sendStatus(204);
        });
      }
    });
  }
});

module.exports = snapshotRouter;