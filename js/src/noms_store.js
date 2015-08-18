'use strict';

require('whatwg-fetch');

var host = function(host) {
  var i = host.indexOf(':');
  return i < 0 ? host : host.substring(0, i);
}(location.host);
var nomsPort = "8000";
var nomsServer = location.protocol + '//' + host + ":" + nomsPort;

var rpc = {
  dataset: nomsServer + '/dataset',
  get: nomsServer + '/get',
  root: nomsServer + '/root',
};

// TODO: Chrome seems to start spitting out uncatchable errors if we queue too many XHRs. This limit probably doesn't actually slow us down because the user agent has its own queue of fetches to service.
var maxConnections = 64;
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
  return requestFetch(rpc.get + '?ref=' + ref);
}

function getRoot() {
  return requestFetch(rpc.root).then(res => res.text());
}

function getDataset(id) {
  return requestFetch(rpc.dataset + '?id=' + id)
}

module.exports = {
  getChunk,
  getDataset,
  getRoot,
};
