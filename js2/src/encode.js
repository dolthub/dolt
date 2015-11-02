/* @flow */

'use strict';

import Chunk from './chunk.js';
import MemoryStore from './memory_store.js';
import Ref from './ref.js';
import type {ChunkStore} from './chunk_store.js';
import type {NomsKind} from './noms_kind.js';
import {isPrimitiveKind, Kind} from './noms_kind.js';
import {makePrimitiveTypeRef, StructDesc, TypeRef} from './type_ref.js';
import {lookupPackage, Package} from './package.js';

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
        t.elemTypes.forEach(elemType => this.writeTypeRefAsTag(elemType));
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
          v.forEach(sv => w2.writeValue(sv, elemType));
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
          elems.forEach(elem => {
            w2.writeValue(elem, keyType);
            w2.writeValue(v.get(elem), valueType);
          });
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
          v.types.forEach(type => w2.writeValue(type, ptr));
          this.write(w2.array);
          let w3 = new JsonArrayWriter(this._cs);
          v.dependencies.forEach(ref => w3.writeRef(ref));
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
          elems.forEach(elem => w2.writeValue(elem, elemType));
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
        t.elemTypes.forEach(elem => w2.writeTypeRefAsValue(elem));
        this.write(w2.array);
        break;
      }
      case Kind.Struct: {
        let desc = t.desc;
        if (desc instanceof StructDesc) {
          this.write(t.name);
          let fieldWriter = new JsonArrayWriter(this._cs);
          desc.fields.forEach(field => {
            fieldWriter.write(field.name);
            fieldWriter.writeTypeRefAsValue(field.t);
            fieldWriter.write(field.optional);
          });
          this.write(fieldWriter.array);
          let choiceWriter = new JsonArrayWriter(this._cs);
          desc.union.forEach(choice => {
            choiceWriter.write(choice.name);
            choiceWriter.writeTypeRefAsValue(choice.t);
            choiceWriter.write(choice.optional);
          });
          this.write(choiceWriter.array);
        } else {
          throw new Error('Attempt to serialize non-struct typeref as struct type-ref');
        }

        break;
      }
      case Kind.Unresolved: {
        let pkgRef = t.packageRef;
        this.writeRef(pkgRef);
        let ordinal = t.ordinal;
        this.write(ordinal);
        if (ordinal === -1) {
          this.write(t.namespace);
          this.write(t.name);
        }

        let pkg = lookupPackage(pkgRef);
        if (pkg) {
          throw new Error('Not implemented');
        }
        break;
      }

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
  return Chunk.fromString(typedTag + JSON.stringify(w.array));
}

export {encodeNomsValue, JsonArrayWriter};
