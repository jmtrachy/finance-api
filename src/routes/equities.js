var express = require('express');
var equityRouter = express.Router();
var dal = require('../dal.js');
var logger = require('../logger.js');

// Set the content type here since it'll always be JS - also just trying to keep code around for having a specific use on a router :)
equityRouter.use(function(req, res, next) {
  res.setHeader('Content-Type', 'application/json');
  next();
});

// ********************************************* Equities ***************************************************

// Route for retrieving a bunch of equities with a filter applied.  Currently filter is required and only works for 'dow'
equityRouter.get('/', async (req, res) => {
  timer = logger.logTiming();
  let filter = req.query.filter;
  if (!filter) {
    let docs = await dal.getAllEquities();
    logger.logTiming('GET all equity', timer);
    res.status(200).send(JSON.stringify(docs));
  } else {
    filter = filter.toLowerCase();
    if (filter != 'dow') {
      res.status(400).send(JSON.stringify({error:'The only options available for the filter parameter are: dow'}));
      logger.logTiming('Completing get of all dow equities (400)', timer);
    } else {
      let docs = await dal.getEquitiesByFilter('dow', true);
      res.status(200).send(JSON.stringify(docs));
      logger.logTiming('Completing get of all dow equities', timer);
    }
  } 
});
                 
// Route for retrieving an equity by Id
equityRouter.get('/:id', async (req, res) => {
  timer = logger.logTiming();
  
  // Get the equity from the id
  let doc = await dal.getEquityById(req.params.id);
 
  if (doc != null) {
    res.status(200).send(JSON.stringify(doc));
    logger.logTiming('GET equity ' + req.params.id, timer);
  } else {
    let errorMessage = {
      error: 'No equity found with id ' + req.params.id
    }
    res.status(404).send(errorMessage);
    logger.logTiming('GET equity (404)' + req.params.id, timer);
  }
});

// Route for creating an equity
equityRouter.post('/', async (req, res) => {
  timer = logger.logTiming();
  let reqBody = req.body;
  logger.log('Post called with ' + JSON.stringify(reqBody));
  
  if (!reqBody.id) {
    let equity = await dal.createEquity(reqBody);
    res.status(201).send(JSON.stringify(equity));
    logger.logTiming('POST equity ' + req.params.id, timer);
  } else {
    res.status(400).send('{ "error": "Update not supported yet" }');
    logger.logTiming('POST equity ' + req.params.id, timer);
  }  
});

// Route for creating an equity by id
equityRouter.put('/:id', async (req, res) => {
  let reqBody = req.body;
  let equityId = req.params.id;
  reqBody.id = equityId;
  logger.log('Put called with ' + JSON.stringify(reqBody));
  
  let equity = dal.createEquity(reqBody);
  res.status(201).send(JSON.stringify(equity));
});

equityRouter.patch('/:id', async (req, res) => {
  var reqBody = req.body;
  var id = req.params.id;
  logger.log('Special patch called for document ' + id + ' and payload ' + JSON.stringify(reqBody));
  
  if (id == null) {
    responseMessage = { "error": "An id must be passed in to identify the individual resource."};
    res.status(400).send(JSON.stringify(responseMessage));
  } else {
    
    let doc = await dal.getEquityById(id);
    
    if (doc == null) {
      res.status(404).send(JSON.stringify({ "error": "No resource found."}));
    } else {
      dal.updateEquity(id, reqBody, function() {
        res.sendStatus(204);
      });
    }
  }
});

// Deletes a particular record by id
equityRouter.delete('/:id', async (req, res) => {
  timer = logger.logTiming();
  var responseCode = null;
  var id = req.params.id;
  
  // TODO: Add check for 32 characters once db is cleaned up
  if (id == null) {
    responseMessage = { "error": "An id must be passed in to identify the individual resource."};
    res.status(400).send(JSON.stringify(responseMessage));
  } else {
    let doc = await dal.getEquityById(id);
    if (doc == null) {
      res.status(404).send(JSON.stringify({ "error": "No resource found."}));
      logger.logTiming('DELETE equity (404)' + req.params.id, timer);
    } else {
      await dal.deleteEquity(id);
      res.sendStatus(204);
      logger.logTiming('DELETE equity (204)' + req.params.id, timer);
    }
  }
});

//******************************************** Snapshots ************************************************

equityRouter.get('/:id/snapshots', async (req, res) => {
  var id = req.params.id;
  timer = logger.logTiming();
  logger.log('id = ' + id);
  if (!id) {
    res.status(400).send(JSON.stringify({error:"'id' is a required path parameter. (example: /v1/equities/AAPL/snapshots)"}));
  } else {
    // Make sure numResults is a valid number
    var numResults = req.query.limit;
    logger.log('numResults = ' + numResults);
    if (typeof numResults === 'undefined') {
      numResults = 1;
    } else {
      numResults = Number(numResults);
    }
    
    let snapshots = await dal.getSnapshotsByEquity(id, numResults);
    logger.log(snapshots)
    logger.logTiming('Completing get of ' + numResults + ' snapshots for equity ' + id, timer);
    res.status(200).send(JSON.stringify(snapshots));
  } 
});

// Route for creating an equity snapshot
equityRouter.post('/:id/snapshots', async (req, res) => {
  timer = logger.logTiming();
  
  var reqBody = req.body;
  logger.log('Post called with ' + JSON.stringify(reqBody));
  
  id = req.params.id;
  if (id.length > 15) {
    reqBody.equityId = id;
    reqBody.ticker = 'AAPL'; // TODO: Remove this obvious hack
  } else {
    reqBody.equityId = 'Brg1g51mUmIVcDM28PAP3dnhoG3O78us'; // TODO: Remove this obvious hack
    reqBody.ticker = id;
  }
  
  if (!reqBody.id) {
    let snapshot = await dal.createSnapshot(reqBody);
    logger.logTiming('POST snapshot for snapshot ' + req.params.id, timer);
    res.status(201).send(JSON.stringify(snapshot));
  } else {
    res.status(400).send('{ "error": "Update not supported yet" }');
  }  
}); 

// Route for retrieving a snapshot by Id
equityRouter.get('/:equityId/snapshots/:id', async(req, res) => {
  timer = logger.logTiming();
  let snapshot = await dal.getSnapshotById(req.params.id);
  
  // Print the length of time it took to produce the response
  logger.logTiming('GET snapshot ' + req.params.id + ' for equity ' + req.params.equityId, timer);
  
  if (snapshot != null) {
    res.status(200).send(JSON.stringify(snapshot));  
  } else {
    var errorMessage = {
      error: 'No snapshot found with id ' + req.params.id
    }
    res.status(404).send(errorMessage);
  }
});

// Deletes a particular record by id
equityRouter.delete('/:equityId/snapshots/:id', async (req, res) => {
  timer = logger.logTiming();
  var responseCode = null;
  var id = req.params.id;
  
  // TODO: Add check for 32 characters once db is cleaned up
  if (id == null) {
    responseMessage = { "error": "An id must be passed in to identify the individual resource."};
    res.status(400).send(JSON.stringify(responseMessage));
  } else {
    let doc = await dal.getSnapshotById(id);
    
    if (doc == null) {
      res.status(404).send(JSON.stringify({ "error": "No resource found."}));
    } else {
      await dal.deleteSnapshotById(id);
      logger.logTiming('Deleting snapshot ' + id, timer);
      res.sendStatus(204);
    }
  }
});

//******************************************** Aggregates ************************************************

// Retrieves aggregates based on a ticker symbol
equityRouter.get('/:id/aggregates', function(req, res) {
  timer = logger.logTiming();
  var id = req.params.id;
  if (!id) {
    res.status(400).send(JSON.stringify({error:"'id' is a required path parameter. It can either be a ticker symbol or an equity id"}));
  } else {
    
    var numResults = req.query.limit;
    logger.log('numResults = ' + numResults);
    if (typeof numResults === 'undefined') {
      numResults = 1;
    } else {
      numResults = Number(numResults);
    }
    
    dal.getAggregatesByEquity(id, numResults, function(docs) {
      logger.logTiming('Completing get of ' + numResults + ' aggregates for equity ' + id, timer);
      res.status(200).send(JSON.stringify(docs));
    });
  } 
});

// Route for retrieving an aggregate by Id
equityRouter.get('/:equityId/aggregates/:id', function(req, res) {
  timer = logger.logTiming();
  dal.getAggregateById(req.params.id, function(doc){
    
    if (doc != null) {
      res.status(200).send(JSON.stringify(doc));
      logger.logTiming('GET aggregate of id ' + req.params.id, timer);
    } else {
      var errorMessage = {
        error: 'No aggregate found with id ' + req.params.id
      }
      res.status(404).send(errorMessage);
    }
    
  });
});

// Route for creating an aggregate
equityRouter.post('/:id/aggregates', async(req, res) => {
  timer = logger.logTiming();
  var reqBody = req.body;  
  id = req.params.id;
  
  if (id.length > 15) {
    reqBody.equityId = id;
    reqBody.ticker = 'AAPL'; // TODO: Remove this obvious hack
  } else {
    reqBody.equityId = 'Brg1g51mUmIVcDM28PAP3dnhoG3O78us'; // TODO: Remove this obvious hack
    reqBody.ticker = id.toUpperCase();
  }
  
  logger.log('Post called with ' + JSON.stringify(reqBody));
  
  if (!reqBody.id) {
    let aggregate = await dal.createAggregate(reqBody);
    res.status(201).send(JSON.stringify(aggregate));
    logger.logTiming('POST aggregate of id ' + req.params.id, timer);
  } else {
    res.status(400).send('{ "error": "Update not supported yet" }');
  }  
});

// Delete an aggregate
equityRouter.delete('/:equityId/aggregates/:id', function(req, res) {
  timer = logger.logTiming();
  equityId = req.params.equityId
  id = req.params.id;
  
  logger.log('Delete called for equity ' + equityId + ' and aggregate ' + id);
  
  if (id == null) {
    responseMessage = { "error": "An id must be passed in to identify the individual resource."};
    res.status(400).send(JSON.stringify(responseMessage));
  } else {
    dal.getAggregateById(id, function(doc) {
      if (doc == null) {
        res.status(404).send(JSON.stringify({ "error": "No resource found."}));
        logger.logTiming('DELETE aggregate of id (404)' + req.params.id, timer);
      } else {
        dal.deleteAggregateById(id, function() {
          res.sendStatus(204);
          logger.logTiming('DELETE aggregate of id ' + req.params.id, timer);
        });
      }
    });
  }
});

module.exports = equityRouter;
