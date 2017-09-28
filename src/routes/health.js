var express = require('express');
var healthRouter = express.Router();
var dal = require('../dal.js');

// Set the content type here since it'll always be JS - also just trying to keep code around for having a specific use on a router :)
healthRouter.use(function(req, res, next) {
  res.setHeader('Content-Type', 'application/json');
  next();
});

// Returns the health of the service
healthRouter.get('/', function(req, res) {
  dal.getAllEquities(function(docs) {
    docsObjectType = Object.prototype.toString.call(docs).slice(8, -1)
    databaseValid = docsObjectType === 'Array'
    healthCheckResponse = {
      'Service': 'Up',
      'Database': databaseValid
    }
    
    res.status(200).send(JSON.stringify(healthCheckResponse) + '\n');
  }); 
});

module.exports = healthRouter;
