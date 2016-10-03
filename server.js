var express = require('express');
var requestId = require('request-id/express');
var bodyParser = require('body-parser');
var dal = require('./dal.js');
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

app.get('/v1/equities/:id', function(req, res) {
  dal.getEquityById(req.params.id, function(docs){
    console.log('Made it all the way back!');
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify(docs));
  });
});

app.listen(port, function() {
  startupMessage = 'Server started, up and listening on port ' + port;
  console.log(startupMessage);
  //options = { description: startupMessage, correlation_id: "Server Startup"};
})