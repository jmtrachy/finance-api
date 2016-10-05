var express = require('express');
var equityRouter = express.Router();
var dal = require('../dal.js');

// Set the content type here since it'll always be JS - also just trying to keep code around for having a specific use on a router :)
equityRouter.use(function(req, res, next) {
  res.setHeader('Content-Type', 'application/json');
  next();
});
                 
// Route for retrieving an equity by Id
equityRouter.get('/:id', function(req, res) {
  dal.getEquityById(req.params.id, function(doc){
    
    if (doc != null) {
      res.status(200).send(JSON.stringify(doc));  
    } else {
      var errorMessage = {
        error: 'No equity found with id ' + req.params.id
      }
      res.status(404).send(errorMessage);
    }
    
  });
});

// Route for creating an equity
equityRouter.post('/', function(req, res) {
  var reqBody = req.body;
  console.log('Post called with ' + JSON.stringify(reqBody));
  
  if (!reqBody.id) {
    dal.createEquity(reqBody, function(equity) {
      res.end(JSON.stringify(equity));
    });
  } else {
    res.status(400).send('{ "error": "Update not supported yet" }');
  }  
});

// Route for updating parts of an equity
equityRouter.put('/:id', function(req, res) {
  var reqBody = req.body;
  var id = req.params.id;
  console.log('Put called for document ' + id + ' and payload ' + JSON.stringify(reqBody));
  
  if (id == null) {
    responseMessage = { "error": "An id must be passed in to identify the individual resource."};
    res.status(400).send(JSON.stringify(responseMessage));
  } else {
    dal.getEquityById(id, function(doc) {
      if (doc == null) {
        res.status(404).send(JSON.stringify({ "error": "No resource found."}));
      } else {
        dal.updateEquity(id, reqBody, function() {
          res.sendStatus(204);
        });
      }
    });
  }
});

// Deletes a particular record by id
equityRouter.delete('/:id', function(req, res) {
  var responseCode = null;
  var id = req.params.id;
  
  // TODO: Add check for 32 characters once db is cleaned up
  if (id == null) {
    responseMessage = { "error": "An id must be passed in to identify the individual resource."};
    res.status(400).send(JSON.stringify(responseMessage));
  } else {
    dal.getEquityById(id, function(doc) {
      if (doc == null) {
        res.status(404).send(JSON.stringify({ "error": "No resource found."}));
      } else {
        dal.deleteEquity(id, function() {
          res.sendStatus(204);
        });
      }
    });
  }
});

module.exports = equityRouter;