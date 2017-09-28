var environmentName = process.env.ENV_NAME;
var env;

var local = {
  databaseLocation: 'localhost'
};

var ci = {
  databaseLocation: 'mongo'
};

var prod = {
  databaseLocation: 'mongo'
};

if (environmentName == 'prod') {
  env = prod;
} else if (environmentName == 'ci') {
  env = mongo;
} else {
  env = local;
}

module.exports = {
  env: env
};