var express = require('express');
var request = require('request');
var app = express();
var fs = require("fs");

function end(res, response, isError) {
  if (isError) console.error(response);
  else console.log(response);
  res.end(response);
}

app.post('/hdinsight/start', function (req, res) {
  request.post(process.env.HDINSIGHT_START_WEBHOOK, function (err, httpResponse, body) {
    if (err) {
      return end(res, { err: 'hdinsight start failed:' + err }, true);
    }

    res.writeHead(200, { 'Content-Type': 'application/json' });    
    end(res, { data: 'request to start hd insight was sent successfully' });
  });
});

app.post('/hdinsight/stop', function (req, res) {
  request.post(process.env.HDINSIGHT_STOP_WEBHOOK, function (err, httpResponse, body) {
    if (err) {
      return end(res, { response: 'hdinsight stop failed:' + err }, true);
    }

    res.writeHead(200, { 'Content-Type': 'application/json' });    
    end(res, { response: 'request to stop hd insight was sent successfully' });
  });
});

app.get('/hdinsight/check', function (req, res) {

  var username = constants.LIVY_USER;
  var password = constants.LIVY_PASS;
  var authenticationHeader = 'Basic ' + new Buffer(username + ':' + password).toString('base64');
  var options = {
    url: constants.HDINSIGHT_CHECK_URL,
    method: 'GET',
    headers: {
      "Content-Type": 'application/json', 
      "Authorization": authenticationHeader 
    }
  };
  request(options, function (error, response, body) {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end({ response: error ? JSON.stringify(error) : body });
  });
});

app.post('/hdinsight/submit-job', function (req, res) {
  var username = process.env.LIVY_USER;
  var password = process.env.LIVY_PASS;
  var authenticationHeader = 'Basic ' + new Buffer(username + ':' + password).toString('base64');
  var options = {
    url: constants.LIVY_URL,
    method: 'POST',
    headers: {
      "Content-Type": 'application/json', 
      "Authorization": authenticationHeader 
    },
    json: req.body
  };
  request(options, function (error, response, body) {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end({ response: error ? JSON.stringify(error) : JSON.stringify(body) });
  });
});

var server = app.listen(process.env.PORT || 3000, function () {

  var host = server.address().address
  var port = server.address().port

  console.log("Example app listening at http://%s:%s", host, port)

});
