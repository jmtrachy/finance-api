var express = require('express');
var authRouter = express.Router();
var dal = require('../dal.js');
var logger = require('../logger.js');

// Set the content type here since it'll always be JS - also just trying to keep code around for having a specific use on a router :)
authRouter.use(function(req, res, next) {
  res.setHeader('Content-Type', 'application/json');
  next();
});

// Route for creating an authentication token
// todo: Add more security based on roles for this call
authRouter.post('/', async (req, res) => {
  timer = logger.logTiming();
  let reqBody = req.body;
  logger.log('Post called with ' + JSON.stringify(reqBody));
  let authToken = reqBody.token;
  
  // Persist the hashed token
  let authTokenResult = await dal.createAuthToken(authToken);

  res.status(201).send();
  logger.logTiming('POST auth token', timer);
});

module.exports = authRouter;