var express = require('express');
var requestId = require('request-id/express');
var bodyParser = require('body-parser');
var dal = require('./dal.js');
var equityRouter = require('./routes/equities.js');
var healthRouter = require('./routes/health.js');
var authRouter = require('./routes/auth.js');
var app = express();

var port = 5000;

// Use a JSON parser - node will automatically detect the type of the request
app.use(bodyParser.json());

app.use(requestId({
  resHeader: 'correlation_id',
  reqHeader: 'correlation_id'
}));

// Logs the request as it comes in
app.use(function(req, res, next) {
//  logging.info(req, res);
// TOOD: complete logging
  next();
});

// Validates credentials
app.use(function(req, res, next) {
  let authenticated = null;
  let token = req.get('Authorization');

  // wrap the call to dal.authenticate in an async block so node allows it to proceed
  (async () => {
    authenticated = await dal.authenticate(token);

    if (authenticated) {
      next();
    } else {
      res.status(403).send(JSON.stringify({'error': 'missing valid authentication token'}));
    }
  })();

});

app.use('/v1/equities', equityRouter);
app.use('/v1/health', healthRouter);
app.use('/v1/auth', authRouter);

dal.initializeDatabases(function() {
  app.listen(port, function() {
    startupMessage = 'Server started, up and listening on port ' + port;
    console.log(startupMessage);
  })
});