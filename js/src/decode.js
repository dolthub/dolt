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

function decodeCompoundBlob(value, ref, getChunk) {
  // {"cb":[{"ref":"sha1-x"},lengthX,{"ref":"sha1-y"},lengthY]}
  return Promise.all(
    value
      .filter(v => typeof v == 'object')
      .map(v => readValue(v.ref, getChunk))
  ).then(values => Promise.resolve(new Blob(values)));
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
  cb: decodeCompoundBlob,
  int8: decodeInt,
  int16: decodeInt,
  int32: decodeInt,
  int64: decodeInt,
  list: decodeList,
  map: decodeMap,
  ref: decodeRef,
  set: decodeSet,
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

function readBlobAsText(blob) {
  return new Promise((res, rej) => {
    var reader = new FileReader();
    reader.addEventListener('loadend',
      () => res(reader.result));
    reader.addEventListener('error', rej);
    reader.readAsText(blob);
  });
}

function readValue(ref, getChunk) {
  return getChunk(ref).then(
    response => response.blob()).then(
    blob => readBlobAsText(blob.slice(0, 2)).then(
      header => {
        var body = blob.slice(2);
        switch (header) {
          case 'j ':
            return readBlobAsText(body).then(
              data => decodeValue(JSON.parse(data), ref, getChunk));
          case 'b ':
            return Promise.resolve(body);
          default :
            throw Error('Unsupported encoding: ' + data[0]);
        }
      }
    )
  );
}

module.exports = {
  readValue: readValue,
  Ref: Ref
};
