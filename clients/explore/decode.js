var Immutable = require('immutable');

function decodeMap(input, getChunk) {
  return new Promise(function(fulfill) {
    Promise.all(input.map(function(value) {
      return decodeValue(value, getChunk);
    })).then(function(values) {
      var pairs = [];
      for (var i = 0; i < input.length; i += 2) {
        pairs.push([values[i], values[i+1]]);
      }
      fulfill(Immutable.Map(pairs));
    });
  });
}

function decodeList(input, getChunk) {
  return new Promise(function(fulfill) {
    Promise.all(input.map(function(value) {
      return decodeValue(value, getChunk);
    })).then(function(values) {
      fulfill(Immutable.List(values));
    });
  });
}

function decodeSet(input, getChunk) {
  return new Promise(function(fulfill) {
    Promise.all(input.map(function(value) {
      return decodeValue(value, getChunk);
    })).then(function(values) {
      fulfill(Immutable.Set(values));
    });
  });
}

function decodeRef(ref, getChunk) {
  return new Promise(function(fulfill) {
    readValue(ref, getChunk).then(fulfill);
  });
}

// TODO(rafael): Kind of cheating to decode all int & float types as numbers.
function decodeTaggedValue(taggedValue, getChunk) {
  return new Promise(function(fulfill) {
    for (var tag in taggedValue) {
      var value = taggedValue[tag];
      switch(tag) {
        case 'int16':
        case 'int32':
        case 'int64':
        case 'uint16':
        case 'uint32':
        case 'uint64':
          fulfill(Number.parseInt(value));
          return;
        case 'float32':
        case 'float64':
          fulfill(Number.parseFloat(value));
          return;
        case 'list':
          decodeList(value, getChunk).then(fulfill);
          return;
        case 'set':
          decodeSet(value, getChunk).then(fulfill);
          return;
        case 'map':
          decodeMap(value, getChunk).then(fulfill);
          return;
        case 'ref':
          decodeRef(value, getChunk).then(fulfill);
          return;
      }

      throw 'Not Reached';
    }
  });
}

function decodeValue(value, getChunk) {
  return new Promise(function(fulfill) {
    if (typeof value != 'object') {
      fulfill(value);
      return;
    }

    decodeTaggedValue(value, getChunk).then(fulfill);
  });
}

function readValue(ref, getChunk) {
  return new Promise(function(fulfill) {
    getChunk(ref).then(function(data) {
      if (data[0] != 'j')
        throw Error('Unsupported encoding');

      var json = JSON.parse(data.substring(2));
      return decodeValue(json, getChunk).then(fulfill);
    });
  });
}

module.exports = {
  readValue: readValue
};
