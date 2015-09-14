'use strict';

require('whatwg-fetch');

var rpc = null;

function setDefaultServer() {
  var host = location.host;
  var i = host.indexOf(':');
  host = i < 0 ? host : host.substring(0, i);
  var nomsPort = 8000;
  var nomsServer = location.protocol + '//' + host + ':' + nomsPort;
  setServer(nomsServer);
}
setDefaultServer();

// Note that chrome limits the number of active xhrs to the same security origin to 6, more than that just sit in the 'stalled' state.
var maxReads = 3;
var activeReads = 0;
var bufferedReads = Object.create(null);
var anyPending = false;
var fetchScheduled = false;

function beginFetch() {
  activeReads++;
  var reqs = bufferedReads;
  bufferedReads = Object.create(null);
  anyPending = false;
  fetchScheduled = false;

  var refs = Object.keys(reqs);
  var body = refs.map(r => 'ref=' + r).join('&');
  fetch(rpc.getRefs, {
    method: 'post',
    body: body,
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded'
    }
  }).then(r => r.blob()).then(deserializeChunks).then((chunks) => {
    // Return success
    Object.keys(chunks).forEach(r => {
      var chunk = chunks[r];
      var callers = reqs[r];
      callers.forEach(c => {
        c.resolve(chunk);
      });

      delete reqs[r];
    });

    // Report failure
    Object.keys(reqs).forEach(r => {
      var callers = reqs[r];
      callers.forEach(c => {
        r.resolve(null);
      })
    });

    endFetch();
  }).catch((err) => {
    Object.keys(reqs).forEach(r => {
      var callers = reqs[r];
      callers.forEach(c => {
        c.reject(err);
      });
    });
  });
}

function endFetch() {
  activeReads--;
  pumpFetchQueue();
}

function pumpFetchQueue() {
  if (!fetchScheduled && anyPending && activeReads < maxReads) {
    fetchScheduled = true;
    setTimeout(beginFetch, 0); // send all requests from this Task in a single request
  }
}

const sha1Size = 20;
const chunkLengthSize = 4; // uint32
const chunkHeaderSize = sha1Size + chunkLengthSize;

function uint8ArrayToRef(a) {
  var ref = 'sha1-';
  for (var i = 0; i < a.length; i++) {
    var v = a[i].toString(16);
    if (v.length == 1) {
      ref += '0' + v;
    } else {
      ref += v;
    }
  }
  return ref;
}

function deserializeChunks(blob) {
  return new Promise((resolve, reject) => {
    var reader = new FileReader();
    reader.addEventListener('loadend', () => {
      var buffer = reader.result;
      var totalLenth = buffer.byteLength;
      var chunks = {};
      for (var i = 0; i < totalLenth;) {
        if (buffer.byteLength - i < chunkHeaderSize) {
          reject('Invalid chunk buffer');
        }

        var sha1Bytes = new Uint8Array(reader.result.slice(i, i + sha1Size));
        i += sha1Size;
        var ref = uint8ArrayToRef(sha1Bytes);

        var length = new Uint32Array(reader.result.slice(i, i + chunkLengthSize))[0];
        i += chunkLengthSize;

        if (i + length > totalLenth) {
          reject('Invalid chunk buffer');
        }

        var chunk = reader.result.slice(i, i + length);
        i += length;
        chunks[ref] = chunk;
      }

      resolve(chunks);
    });

    reader.readAsArrayBuffer(blob);
  });
}

function getChunk(ref) {
  return new Promise((resolve, reject) => {
    var callers = bufferedReads[ref] || [];
    callers.push({ resolve, reject });
    bufferedReads[ref] = callers;
    anyPending = true;
    pumpFetchQueue();
  });
}

function getRoot() {
  return new Promise((resolve, reject) => {
    fetch(rpc.root).then(r => r.text()).then(resolve).catch(reject);
  });
}

function setServer(url) {
  rpc = {
    getRefs: url + '/getRefs/',
    ref: url + '/ref',
    root: url + '/root',
  };
}

module.exports = {
  getChunk,
  getRoot,
  setServer,
};
