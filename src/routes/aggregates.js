var express = require('express');
var aggregateRouter = express.Router();
var dal = require('../dal.js');

// Set the content type here since it'll always be JS - also just trying to keep code around for having a specific use on a router :)
aggregateRouter.use(function(req, res, next) {
  res.setHeader('Content-Type', 'application/json');
  next();
});

aggregateRouter.get('/', function(req, res) {
  var ticker = req.query.ticker;
  if (!ticker) {
    res.status(400).send(JSON.stringify({error:"'ticker' is a required query parameter."}));
  } else {
    ticker = ticker.toUpperCase();
    var numResults = req.query.limit;
    console.log('numResults = ' + numResults);
    if (typeof numResults === 'undefined') {
      numResults = 1;
    } else {
      numResults = Number(numResults);
    }
    dal.getAggregatesByTicker(ticker, numResults, function(docs) {
      res.status(200).send(JSON.stringify(docs));
    });
  } 
});

// Route for creating an equity
aggregateRouter.post('/', function(req, res) {
  var reqBody = req.body;
  console.log('Post called with ' + JSON.stringify(reqBody));
  
  if (!reqBody.id) {
    dal.createAggregate(reqBody, function(aggregate) {
      res.end(JSON.stringify(aggregate));
    });
  } else {
    res.status(400).send('{ "error": "Update not supported yet" }');
  }  
});

module.exports = aggregateRouter;