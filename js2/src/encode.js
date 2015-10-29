/* @flow */

'use strict';

import type {ChunkStore} from './chunk_store.js';
import type {NomsKind} from './noms_kind.js';

import Chunk from './chunk.js';
import MemoryStore from './memory_store.js';
import {Kind} from './noms_kind.js';
import {TypeRef} from './type_ref.js';

const typedTag = 't ';

class JsonArrayWriter {
  array: Array<any>;
  _cs: ChunkStore;

  constructor(cs: ChunkStore) {
    this.array = [];
    this._cs = cs;
  }

  write(v: any) {
    this.array.push(v);
  }

  writeKind(k: NomsKind) {
    this.write(k);
  }

  writeTypeRefAsTag(t: TypeRef) {
    let k = t.kind;
    this.writeKind(k);
    switch (k) {
    case Kind.Enum:
    case Kind.Struct:
      throw new Error('Unreachable');
    case Kind.List:
    case Kind.Map:
    case Kind.Ref:
    case Kind.Set: {
      let elemTypes = t.elemTypes;
      for (let i = 0; i < elemTypes.length; i++) {
        this.writeTypeRefAsTag(elemTypes[i]);
      }
      break;
    }
    case Kind.Unresolved:
      throw new Error('Not implemented');
    }
  }

  writeTopLevel(t: TypeRef, v: any) {
    this.writeTypeRefAsTag(t);
    this.writeValue(v, t);
  }

  writeValue(v: any, t: TypeRef) {
    switch (t.kind) {
    case Kind.Blob:
      throw new Error('Not implemented');
    case Kind.Bool:
    case Kind.UInt8:
    case Kind.UInt16:
    case Kind.UInt32:
    case Kind.UInt64:
    case Kind.Int8:
    case Kind.Int16:
    case Kind.Int32:
    case Kind.Int64:
    case Kind.Float32:
    case Kind.Float64:
    case Kind.String:
      this.write(v); // TODO: Verify value fits in type
      break;
    case Kind.List: {
      let w2 = new JsonArrayWriter(this._cs);
      let elemType = t.elemTypes[0];
      if (v instanceof Array) {
        for (let i = 0; i < v.length; i++) {
          w2.writeValue(v[i], elemType);
        }
      } else {
        throw new Error('Attempt to serialize non-list as list');
      }

      this.write(w2.array);
      break;
    }
    case Kind.Set: {
      let w2 = new JsonArrayWriter(this._cs);
      let elemType = t.elemTypes[0];
      if (v instanceof Set) {
        let elems = [];
        v.forEach(v => {
          elems.push(v);
        });
        elems = orderValuesByRef(elemType, elems);
        for (let i = 0; i < elems.length; i++) {
          w2.writeValue(elems[i], elemType);
        }
      } else {
        throw new Error('Attempt to serialize non-set as set');
      }

      this.write(w2.array);
      break;
    }
    case Kind.Map: {
      let w2 = new JsonArrayWriter(this._cs);
      let keyType = t.elemTypes[0];
      let valueType = t.elemTypes[1];
      if (v instanceof Map) {
        let elems = [];
        v.forEach((v, k) => {
          elems.push(k);
        });
        elems = orderValuesByRef(keyType, elems);
        for (let i = 0; i < elems.length; i++) {
          w2.writeValue(elems[i], keyType);
          w2.writeValue(v.get(elems[i]), valueType);
        }
      } else {
        throw new Error('Attempt to serialize non-map as maps');
      }

      this.write(w2.array);
      break;
    }
    default:
      throw new Error('Not implemented');
    }
  }
}

function orderValuesByRef(t: TypeRef, a: Array<any>): Array<any> {
  return a.map(v => {
    return {
      v: v,
      chunk: encodeNomsValue(v, t)
    };
  }).sort((a, b) => {
    return a.chunk.ref.compare(b.chunk.ref);
  }).map(o => {
    return o.v;
  });
}

function encodeNomsValue(v: any, t: TypeRef): Chunk {
  let ms = new MemoryStore(); // TODO: This should be passed in.
  let w = new JsonArrayWriter(ms);
  w.writeTopLevel(t, v);
  return new Chunk(typedTag + JSON.stringify(w.array));
}

module.exports = {JsonArrayWriter};
