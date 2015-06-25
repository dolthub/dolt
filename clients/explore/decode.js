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

function decodeInt(value) {
  return new Promise(function(fulfill) {
    fulfill(Number.parseInt(value));
  });
}

function decodeFloat(value) {
  return new Promise(function(fulfill) {
    fulfill(Number.parseFloat(value));
  });
}

var decode = {
  map: decodeMap,
  list: decodeList,
  set: decodeSet,
  ref: decodeRef,
  int16: decodeInt,
  int32: decodeInt,
  int64: decodeInt,
  uint16: decodeInt,
  uint32: decodeInt,
  uint64: decodeInt,
  float32: decodeFloat,
  float64: decodeFloat
};

// TODO: Kind of cheating to decode all int & float types as numbers.
function decodeTaggedValue(taggedValue, getChunk) {
  return new Promise(function(fulfill) {
    var tagValue = [];
    for (var tag in taggedValue) {
      tagValue.push(tag, taggedValue[tag]);
    }

    if (tagValue.length != 2) {
      throw Error('Bad tagged value encoding');
    }

    decodeFn = decode[tagValue[0]];
    if (!decodeFn) {
      throw Error('Unknown tagged value: ' + tagValue[0]);
    }

    decodeFn(tagValue[1], getChunk).then(fulfill);
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
