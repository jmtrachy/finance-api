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

// Route for retrieving an equity by Id
app.get('/v1/equities/:id', function(req, res) {
  dal.getEquityById(req.params.id, function(doc){
    res.setHeader('Content-Type', 'application/json');
    
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
app.post('/v1/equities', function(req, res) {
  var reqBody = req.body;
  console.log(JSON.stringify(reqBody));
  if (!reqBody.id) {
    dal.createEquity(reqBody, function(equity) {
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify(equity));
    });
  } else {
    res.setHeader('Content-Type', 'application/json');
    res.status(400).send('{ "error": "Update not supported yet" }');
  }
  
  //dal.createEquity(req.body)
});

app.listen(port, function() {
  startupMessage = 'Server started, up and listening on port ' + port;
  console.log(startupMessage);
  //options = { description: startupMessage, correlation_id: "Server Startup"};
})