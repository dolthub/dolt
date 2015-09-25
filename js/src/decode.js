'use strict';

var Immutable = require('immutable');
var TextDecoder = require('text-encoding').TextDecoder; // Polyfill for Safari

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

  equals(other) {
    return this.ref === other.ref;
  }

  hashCode() {
    return parseInt(this.ref.slice(-8), 16);
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

function decodeType(input, ref, getChunk) {
  // {"type":{"desc":{"map":["fields",{"ref":"sha1-..."}]},"kind":{"uint8":19},"name":"Package"}}
  // {"type":{"kind":{"uint8":13},"name":"Commit","pkgRef":{"ref":"sha1-..."}}}
  return new Ref(ref, () => {
    // input is a js object where the values are encoded. Decode all the values
    // and create an Immutable.Map when the decoding of these values have been
    // resolved.
    let keys = Object.keys(input);
    let map = Immutable.Map().asImmutable();
    return Promise.all(keys.map(k => decodeValue(input[k], ref, getChunk)))
        .then(values => {
          values.forEach((v, i) => {
            map = map.set(keys[i], v);
          });
          return map.asImmutable();
        });
  });
}

function decodeCompoundBlob(value, ref, getChunk) {
  // {"cb":["sha1-x",lengthX,"sha1-y",lengthY]}
  return new Ref(ref, () => {
    return Promise.all(
        value
            .filter((v, i) => i % 2 === 0)
            .map(v => readValue(v, getChunk)))
        .then(childBlobs => new Blob(childBlobs));
  });
}

function decodeCompoundList(value, ref, getChunk) {
  // {"cl":["sha1-x",lengthX,"sha1-y",lengthY]}
  return new Ref(ref, () => {
    return Promise.all(
        value
            .filter((v, i) => i % 2 === 0)
            // v is a string representing the ref here.
            .map(v => decodeRef(v, ref, getChunk).deref()))
        .then(childLists => Immutable.List(childLists).flatten(1));
  });
}

function decodeRef(ref, _, getChunk) {
  return new Ref(ref, () => {
    return readValue(ref, getChunk);
  });
}

function decodeInt(value) {
  return Promise.resolve(parseInt(value));
}

function decodeFloat(value) {
  return Promise.resolve(parseFloat(value));
}

var decode = {
  cb: decodeCompoundBlob,
  cl: decodeCompoundList,
  int8: decodeInt,
  int16: decodeInt,
  int32: decodeInt,
  int64: decodeInt,
  list: decodeList,
  map: decodeMap,
  ref: decodeRef,
  set: decodeSet,
  type: decodeType,
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

var textDecoder = new TextDecoder();

function readValue(ref, getChunk) {
  return getChunk(ref)
      .then(chunk => {
        var hBytes = new Uint8Array(chunk.slice(0, 2));
        var header = String.fromCharCode(hBytes[0], hBytes[1]);
        var body = chunk.slice(2);

        switch (header) {
          case 'j ':
            return decodeValue(JSON.parse(textDecoder.decode(body)), ref, getChunk)
          case 'b ':
            return body;
          default :
            throw Error('Unsupported encoding: ' + header);
        }
      });
}

module.exports = {
  readValue: readValue,
  Ref: Ref
};
