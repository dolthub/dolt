'use strict';

var Immutable = require('immutable');

var refMapping = new WeakMap();

function decodeMap(input, ref, getChunk) {
  return Promise.all(input.map(function(value) {
    return decodeValue(value, ref, getChunk);
  })).then(function(values) {
    var pairs = [];
    for (var i = 0; i < input.length; i += 2) {
      pairs.push([values[i], values[i+1]]);
    }
    var value = Immutable.Map(pairs);
    refMapping.set(value, ref);
    return value;
  });
}

function decodeList(input, ref, getChunk) {
  return Promise.all(input.map(function(value) {
    return decodeValue(value, ref, getChunk);
  })).then(function(values) {
    var value = Immutable.List(values);
    refMapping.set(value, ref);
    return value;
  });
}

function decodeSet(input, ref, getChunk) {
  return Promise.all(input.map(function(value) {
    return decodeValue(value, ref, getChunk);
  })).then(function(values) {
    var value = Immutable.Set(values);
    refMapping.set(value, ref);
    return value;
  });
}

function decodeRef(ref, _, getChunk) {
  return readValue(ref, getChunk);
}

function decodeInt(value) {
  return Promise.resolve(Number.parseInt(value));
}

function decodeFloat(value) {
  return Promise.resolve(Number.parseFloat(value));
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
function decodeTaggedValue(taggedValue, ref, getChunk) {
  var tagValue = [];
  for (var tag in taggedValue) {
    tagValue.push(tag, taggedValue[tag]);
  }

  if (tagValue.length !== 2) {
    return Promise.reject(new Error('Bad tagged value encoding'));
  }

  var decodeFn = decode[tagValue[0]];
  if (!decodeFn) {
    return Promise.reject(new Error('Unknown tagged value: ' + tagValue[0]));
  }

  return decodeFn(tagValue[1], ref, getChunk);
}

function decodeValue(value, ref, getChunk) {
  if (typeof value !== 'object') {
    return Promise.resolve(value);
  }

  return decodeTaggedValue(value, ref, getChunk);
}

function readValue(ref, getChunk) {
  return getChunk(ref).then(function(data) {
    switch(data[0]) {
      case 'j':
        var json = JSON.parse(data.substring(2))
        return decodeValue(json, ref, getChunk);
      case 'b':
        return decodeValue("(blob) ref: " + ref, ref, getChunk);
      default :
        throw Error('Unsupported encoding: ' + data[0]);
    }
  });
}

function getRef(value) {
  return typeof value !== 'object' ? undefined : refMapping.get(value);
}

module.exports = {
  readValue: readValue,
  getRef: getRef,
};
