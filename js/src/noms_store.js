'use strict';

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

// TODO: Use whatwg-fetch
function fetch(url) {
  return new Promise((resolve, reject) => {
    var xhr = new XMLHttpRequest();
    xhr.onload = (e) => {
      resolve(e.target.responseText);
    };
    xhr.onerror = (e) => {
      reject(e.target.statusText);
    };
    xhr.open('get', url, true);
    xhr.send();
  });
}

function getChunk(ref) {
  return fetch(rpc.get + '?ref=' + ref);
}

function getRoot() {
  return fetch(rpc.root);
}

function getDataset(id) {
  return fetch(rpc.dataset + '?id=' + id)
}

module.exports = {
  getChunk,
  getDataset,
  getRoot,
};
