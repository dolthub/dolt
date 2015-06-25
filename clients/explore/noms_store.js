var host = function(host) {
  var i = host.indexOf(':');
  return i < 0 ? host : host.substring(0, i);
}(location.host);
var nomsPort = "8000";
var nomsServer = location.protocol + '//' + host + ":" + nomsPort;

var rpc = {
  get: nomsServer + '/get',
  root: nomsServer + '/root'
}

// TODO(rafael): Use whatwg-fetch
function fetch(url) {
  return new Promise(function(fulfill) {
    var xhr = new XMLHttpRequest();
    xhr.addEventListener('load', function(e) {
      fulfill(e.target.responseText);
    });
    xhr.open("get", url, true);
    xhr.send();
  });
}

function getChunk(ref) {
  return fetch(rpc.get + '?ref=' + ref);
}

function getRoot() {
  return fetch(rpc.root);
}

module.exports = {
  getRoot: getRoot,
  getChunk: getChunk
};
