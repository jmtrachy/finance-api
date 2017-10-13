var config = require('./config/config.js');

var log = function(message, forceLog) {
  if (config.logVerbose || forceLog) {
    console.log(message);
  }
}

var logTiming = function(event, startTimer) {
  logTimer = null;
  if (config.logTimings) {
    logTimer = currentTime = new Date().getTime();
    if (startTimer != null) {
      totalMillis = logTimer - startTimer
      console.log(event + ' took ' + totalMillis + ' millis');
    }
  }
  return logTimer;
}

module.exports = {
  log: log,
  logTiming: logTiming
};