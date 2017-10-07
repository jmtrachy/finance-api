var express = require('express');
var equityRouter = express.Router();
var dal = require('../dal.js');

// Set the content type here since it'll always be JS - also just trying to keep code around for having a specific use on a router :)
equityRouter.use(function(req, res, next) {
  res.setHeader('Content-Type', 'application/json');
  next();
});

// Route for retrieving a bunch of equities with a filter applied.  Currently filter is required and only works for 'dow'
equityRouter.get('/', function(req, res) {
  var filter = req.query.filter;
  if (!filter) {
    dal.getAllEquities(function(docs) {
      res.status(200).send(JSON.stringify(docs));
    });
  } else {
    filter = filter.toLowerCase();
    if (filter != 'dow') {
      res.status(400).send(JSON.stringify({error:'The only options available for the filter parameter are: dow'}));
    } else {
      dal.getEquitiesByFilter('dow', true, function(docs) {
        res.status(200).send(JSON.stringify(docs));
      });
    }
  } 
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

// Route for creating an equity by id
equityRouter.put('/:id', function(req, res) {
  var reqBody = req.body;
  var equityId = req.params.id;
  reqBody.id = equityId;
  console.log('Put called with ' + JSON.stringify(reqBody));
  
  dal.createEquity(reqBody, function(equity) {
    res.end(JSON.stringify(equity));
  });
});

// Route for updating parts of an equity
equityRouter.patch('/:id', function(req, res) {
  var reqBody = req.body;
  var id = req.params.id;
  console.log('Patch called for document ' + id + ' and payload ' + JSON.stringify(reqBody));
  
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

equityRouter.get('/:id/snapshots', function(req, res) {
  var id = req.params.id;
  console.log('id = ' + id);
  if (!id) {
    res.status(400).send(JSON.stringify({error:"'id' is a required path parameter. (example: /v1/equities/AAPL/snapshots)"}));
  } else {
    // Make sure numResults is a valid number
    var numResults = req.query.limit;
    console.log('numResults = ' + numResults);
    if (typeof numResults === 'undefined') {
      numResults = 1;
    } else {
      numResults = Number(numResults);
    }
    
    dal.getSnapshotsByEquity(id, numResults, function(docs) {
      console.log(docs)
      if (docs.length > 0) {
        res.status(200).send(JSON.stringify(docs));
      } else {
        res.status(404).send('{"errorMessage": "snapshots not found for equity ' + id + '"}');
      }
    });
  } 
});

// Route for creating an equity snapshot
equityRouter.post('/:id/snapshots', function(req, res) {
  var reqBody = req.body;
  console.log('Post called with ' + JSON.stringify(reqBody));
  
  id = req.params.id;
  if (id.length > 15) {
    reqBody.equityId = id;
    reqBody.ticker = 'AAPL'; // TODO: Remove this obvious hack
  } else {
    reqBody.equityId = 'Brg1g51mUmIVcDM28PAP3dnhoG3O78us'; // TODO: Remove this obvious hack
    reqBody.ticker = id;
  }
  
  if (!reqBody.id) {
    dal.createSnapshot(reqBody, function(snapshot) {
      res.end(JSON.stringify(snapshot));
    });
  } else {
    res.status(400).send('{ "error": "Update not supported yet" }');
  }  
});

// Route for retrieving a snapshot by Id
equityRouter.get('/:equityId/snapshots/:id', function(req, res) {
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
equityRouter.delete('/:equityId/snapshots/:id', function(req, res) {
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

// Retrieves aggregates based on a ticker symbol
equityRouter.get('/:id/aggregates', function(req, res) {
  var id = req.params.id;
  if (!id) {
    res.status(400).send(JSON.stringify({error:"'id' is a required path parameter. It can either be a ticker symbol or an equity id"}));
  } else {
    
    var numResults = req.query.limit;
    console.log('numResults = ' + numResults);
    if (typeof numResults === 'undefined') {
      numResults = 1;
    } else {
      numResults = Number(numResults);
    }
    
    dal.getAggregatesByEquity(id, numResults, function(docs) {
      res.status(200).send(JSON.stringify(docs));
    });
  } 
});

// Route for retrieving an aggregate by Id
equityRouter.get('/:equityId/aggregates/:id', function(req, res) {
  dal.getAggregateById(req.params.id, function(doc){
    
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

// Route for creating an aggregate
equityRouter.post('/:id/aggregates', function(req, res) {
  var reqBody = req.body;  
  id = req.params.id;
  
  if (id.length > 15) {
    reqBody.equityId = id;
    reqBody.ticker = 'AAPL'; // TODO: Remove this obvious hack
  } else {
    reqBody.equityId = 'Brg1g51mUmIVcDM28PAP3dnhoG3O78us'; // TODO: Remove this obvious hack
    reqBody.ticker = id.toUpperCase();
  }
  
  console.log('Post called with ' + JSON.stringify(reqBody));
  
  if (!reqBody.id) {
    dal.createAggregate(reqBody, function(aggregate) {
      res.end(JSON.stringify(aggregate));
    });
  } else {
    res.status(400).send('{ "error": "Update not supported yet" }');
  }  
});

// Delete an aggregate
equityRouter.delete('/:equityId/aggregates/:id', function(req, res) {
  equityId = req.params.equityId
  id = req.params.id;
  
  console.log('Delete called for equity ' + equityId + ' and aggregate ' + id);
  
  if (id == null) {
    responseMessage = { "error": "An id must be passed in to identify the individual resource."};
    res.status(400).send(JSON.stringify(responseMessage));
  } else {
    dal.getAggregateById(id, function(doc) {
      if (doc == null) {
        res.status(404).send(JSON.stringify({ "error": "No resource found."}));
      } else {
        dal.deleteAggregateById(id, function() {
          res.sendStatus(204);
        });
      }
    });
  }
});

module.exports = equityRouter;
