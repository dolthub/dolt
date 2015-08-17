'use strict';

var Immutable = require('immutable');

var isRef = {};

class Ref{
  constructor(ref, fetchFn) {
    this._isRef = isRef;
    this.ref = ref;
    this._fetch = fetchFn;
    this._promise = null;
  }

  deref() {
    if (this._promise === null) {
      this._promise = recursiveRef(this);
    }

    return this._promise;
  }

  // BUG 88 (instance of is failing in dev build)
  static isRef(ref) {
    return ref && ref._isRef === isRef;
  }
}

function recursiveRef(ref) {
  if (Ref.isRef(ref)) {
    return ref._fetch().then(recursiveRef);
  }

  return Promise.resolve(ref);
}

function decodeMap(input, ref, getChunk) {
  return new Ref(ref, () => {
    return Promise.all(input.map((value) => {
      return decodeValue(value, ref, getChunk);
    })).then((values) => {
      var pairs = [];
      for (var i = 0; i < input.length; i += 2) {
        pairs.push([values[i], values[i+1]]);
      }
      var value = Immutable.Map(pairs);
      return value;
    });
  });
}

function decodeList(input, ref, getChunk) {
  return new Ref(ref, () => {
    return Promise.all(input.map((value) => {
      return decodeValue(value, ref, getChunk);
    })).then((values) => {
      var value = Immutable.List(values);
      return value;
    });
  });
}

function decodeSet(input, ref, getChunk) {
  return new Ref(ref, () => {
    return Promise.all(input.map((value) => {
      return decodeValue(value, ref, getChunk);
    })).then((values) => {
      var value = Immutable.Set(values);
      return value;
    });
  });
}

function decodeRef(ref, _, getChunk) {
  return new Ref(ref, () => {
    return readValue(ref, getChunk);
  });
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
  int8: decodeInt,
  int16: decodeInt,
  int32: decodeInt,
  int64: decodeInt,
  uint8: decodeInt,
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
  return getChunk(ref).then((data) => {
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

module.exports = {
  readValue: readValue,
  Ref: Ref
};
