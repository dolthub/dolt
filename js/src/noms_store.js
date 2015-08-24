'use strict';

require('whatwg-fetch');

var host = function(host) {
  var i = host.indexOf(':');
  return i < 0 ? host : host.substring(0, i);
}(location.host);
var nomsPort = "8000";
var nomsServer = location.protocol + '//' + host + ":" + nomsPort;

var rpc = {
  ref: nomsServer + '/ref',
  root: nomsServer + '/root',
};

// Note that chrome limits the number of active xhrs to the same security origin to 6, more than that just sit in the "stalled" state.
var maxConnections = 8;
var activeFetches = 0;
var pendingFetches = [];

function requestFetch(url) {
  return new Promise((resolve, reject) => {
    pendingFetches.push({ url, resolve, reject });
    pumpFetchQueue();
  });
}

function beginFetch(req) {
  activeFetches++;
  fetch(req.url).then((r) => {
    // TODO: The caller should be able to do additional async work before endFetch() is called.
    req.resolve(r);
    endFetch();
  }).catch(req.reject);
}

function endFetch() {
  activeFetches--;
  pumpFetchQueue();
}

function pumpFetchQueue() {
  while (pendingFetches.length && activeFetches < maxConnections) {
    beginFetch(pendingFetches.shift());
  }
}

function getChunk(ref) {
  return requestFetch(rpc.ref + '/' + ref);
}

function getRoot() {
  return requestFetch(rpc.root).then(res => res.text());
}

module.exports = {
  getChunk,
  getRoot,
};
