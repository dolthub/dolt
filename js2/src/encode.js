/* @flow */

'use strict';

import Chunk from './chunk.js';
import MemoryStore from './memory_store.js';
import Ref from './ref.js';
import type {ChunkStore} from './chunk_store.js';
import type {NomsKind} from './noms_kind.js';
import {isPrimitiveKind, Kind} from './noms_kind.js';
import {makePrimitiveTypeRef, StructDesc, TypeRef} from './type_ref.js';
import {Package} from './package.js';

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

  writeRef(r: Ref) {
    this.write(r.toString());
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
        if (v instanceof Array) {
          let w2 = new JsonArrayWriter(this._cs);
          let elemType = t.elemTypes[0];
          for (let i = 0; i < v.length; i++) {
            w2.writeValue(v[i], elemType);
          }
          this.write(w2.array);
        } else {
          throw new Error('Attempt to serialize non-list as list');
        }

        break;
      }
      case Kind.Map: {
        if (v instanceof Map) {
          let w2 = new JsonArrayWriter(this._cs);
          let keyType = t.elemTypes[0];
          let valueType = t.elemTypes[1];
          let elems = [];
          v.forEach((v, k) => {
            elems.push(k);
          });
          elems = orderValuesByRef(keyType, elems);
          for (let i = 0; i < elems.length; i++) {
            w2.writeValue(elems[i], keyType);
            w2.writeValue(v.get(elems[i]), valueType);
          }
          this.write(w2.array);
        } else {
          throw new Error('Attempt to serialize non-map as maps');
        }

        break;
      }
      case Kind.Package: {
        if (v instanceof Package) {
          let ptr = makePrimitiveTypeRef(Kind.TypeRef);
          let w2 = new JsonArrayWriter(this._cs);
          for (let i = 0; i < v.types.length; i++) {
            w2.writeValue(v.types[i], ptr);
          }
          this.write(w2.array);
          let w3 = new JsonArrayWriter(this._cs);
          for (let i = 0; i < v.dependencies.length; i++) {
            w3.writeRef(v.dependencies[i]);
          }
          this.write(w3.array);
        } else {
          throw new Error('Attempt to serialize non-package as package');
        }

        break;
      }
      case Kind.Set: {
        if (v instanceof Set) {
          let w2 = new JsonArrayWriter(this._cs);
          let elemType = t.elemTypes[0];
          let elems = [];
          v.forEach(v => {
            elems.push(v);
          });
          elems = orderValuesByRef(elemType, elems);
          for (let i = 0; i < elems.length; i++) {
            w2.writeValue(elems[i], elemType);
          }
          this.write(w2.array);
        } else {
          throw new Error('Attempt to serialize non-set as set');
        }

        break;
      }
      case Kind.TypeRef:
        if (v instanceof TypeRef) {
          this.writeTypeRefAsValue(v);
        } else {
          throw new Error('Attempt to serialize non-typeref as typeref');
        }
        break;
      default:
        throw new Error('Not implemented');
    }
  }

  writeTypeRefAsValue(t: TypeRef) {
    let k = t.kind;
    this.writeKind(k);
    switch (k) {
      case Kind.Enum:
        throw new Error('Not implemented');
      case Kind.List:
      case Kind.Map:
      case Kind.Ref:
      case Kind.Set: {
        this.write(t.name);
        let w2 = new JsonArrayWriter(this._cs);
        for (let i = 0; t.elemTypes.length; i++) {
          w2.writeTypeRefAsValue(t.elemTypes[i]);
        }
        this.write(w2.array);
        break;
      }
      case Kind.Struct: {
        let desc = t.desc;
        if (desc instanceof StructDesc) {
          this.write(t.name);
          let fieldWriter = new JsonArrayWriter(this._cs);
          for (let i = 0; i < desc.fields.length; i++) {
            let field = desc.fields[i];
            fieldWriter.write(field.name);
            fieldWriter.writeTypeRefAsValue(field.t);
            fieldWriter.write(field.optional);
          }
          this.write(fieldWriter.array);
          let choiceWriter = new JsonArrayWriter(this._cs);
          for (let i = 0; i < desc.union.length; i++) {
            let choice = desc.union[i];
            choiceWriter.write(choice.name);
            choiceWriter.writeTypeRefAsValue(choice.t);
            choiceWriter.write(choice.optional);
          }
          this.write(choiceWriter.array);
        } else {
          throw new Error('Attempt to serialize non-struct typeref as struct type-ref');
        }

        break;
      }
      case Kind.Unresolved:
        throw new Error('Not implemented');
      default: {
        if (!isPrimitiveKind(k)) {
          throw new Error('Not implemented.');
        }
      }
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
  // if (v instanceof Package) {
  //   if (v.dependencies.length > 0) {
  //     throw new Error('Not implemented');
  //   }
  // }

  let ms = new MemoryStore(); // TODO: This should be passed in.
  let w = new JsonArrayWriter(ms);
  w.writeTopLevel(t, v);
  return new Chunk(typedTag + JSON.stringify(w.array));
}

export {encodeNomsValue, JsonArrayWriter};
