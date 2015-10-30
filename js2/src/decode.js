/* @flow */

'use strict';

import Chunk from './chunk.js';
import MemoryStore from './memory_store.js';
import Ref from './ref.js';
import type {ChunkStore} from './chunk_store.js';
import type {NomsKind} from './noms_kind.js';
import {isPrimitiveKind, Kind} from './noms_kind.js';
import {lookupPackage, Package} from './package.js';
import {makeCompoundTypeRef, makePrimitiveTypeRef, makeTypeRef, StructDesc, TypeRef} from './type_ref.js';

const typedTag = 't ';

class JsonArrayReader {
  _a: Array<any>;
  _i: number;
  _cs: ChunkStore;

  constructor(a: Array<any>, cs: ChunkStore) {
    this._a = a;
    this._i = 0;
    this._cs = cs;
  }

  read(): any {
    return this._a[this._i++];
  }

  atEnd(): boolean {
    return this._i >= this._a.length;
  }

  readString(): string {
    let next = this.read();
    if (typeof next === 'string') {
      return next;
    }
    throw new Error('Serialization error: expected string, got ' + typeof next);
  }

  readBool(): boolean {
    let next = this.read();
    if (typeof next === 'boolean') {
      return next;
    }
    throw new Error('Serialization error: expected boolean, got ' + typeof next);
  }

  readOrdinal(): number {
    let next = this.read();
    if (typeof next === 'number' && next >= 0) {
      return next;
    }

    throw new Error('Serialization error: expected ordinal, got ' + typeof next);
  }

  readArray(): Array<any> {
    let next = this.read();
    if (next instanceof Array) {
      return next;
    }

    throw new Error('Serialization error: expected Array');
  }

  readKind(): NomsKind {
    let next = this.read();
    if (typeof next === 'number') {
      return next;
    }
    throw new Error('Serialization error: expected NomsKind, got ' + typeof next);
  }

  readRef(): Ref {
    let next = this.read();
    if (typeof next === 'string') {
      return Ref.parse(next);
    }

    throw new Error('Serialization error: expected Ref, got ' + typeof next);
  }

  readTypeRefAsTag(): TypeRef {
    let kind = this.readKind();
    switch (kind) {
      case Kind.List:
      case Kind.Set:
      case Kind.Ref: {
        let elemType = this.readTypeRefAsTag();
        return makeCompoundTypeRef(kind, elemType);
      }
      case Kind.Map: {
        let keyType = this.readTypeRefAsTag();
        let valueType = this.readTypeRefAsTag();
        return makeCompoundTypeRef(kind, keyType, valueType);
      }
      case Kind.TypeRef:
        return makePrimitiveTypeRef(Kind.TypeRef);
      case Kind.Unresolved: {
        let pkgRef = this.readRef();
        let ordinal = this.readOrdinal();
        return makeTypeRef(pkgRef, ordinal);
      }
    }

    if (isPrimitiveKind(kind)) {
      return makePrimitiveTypeRef(kind);
    }

    throw new Error('Unreachable');
  }

  readList(t: TypeRef, pkg: ?Package): Array<any> {
    let elemType = t.elemTypes[0];
    let list = [];
    while (!this.atEnd()) {
      let v = this.readValueWithoutTag(elemType, pkg);
      list.push(v);
    }
    return list;
  }

  readSet(t: TypeRef, pkg: ?Package): Set {
    let elemType = t.elemTypes[0];
    let s = new Set();
    while (!this.atEnd()) {
      let v = this.readValueWithoutTag(elemType, pkg);
      s.add(v);
    }

    return s;
  }

  readMap(t: TypeRef, pkg: ?Package): Map {
    let keyType = t.elemTypes[0];
    let valueType = t.elemTypes[1];
    let m = new Map();
    while (!this.atEnd()) {
      let k = this.readValueWithoutTag(keyType, pkg);
      let v = this.readValueWithoutTag(valueType, pkg);
      m.set(k, v);
    }

    return m;
  }

  readTopLevelValue(): any {
    let t = this.readTypeRefAsTag();
    return this.readValueWithoutTag(t);
  }

  readValueWithoutTag(t: TypeRef, pkg: ?Package = null): any {
    // TODO: Verify read values match tagged kinds.
    switch (t.kind) {
      case Kind.Blob:
        throw new Error('Not implemented');
      case Kind.Bool:
        return this.readBool();
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
        return this.read();
      case Kind.String:
        return this.readString();
      case Kind.Value: {
        let t2 = this.readTypeRefAsTag();
        return this.readValueWithoutTag(t2, pkg);
      }
      case Kind.List: {
        let r2 = new JsonArrayReader(this.readArray(), this._cs);
        return r2.readList(t, pkg);
      }
      case Kind.Map: {
        let r2 = new JsonArrayReader(this.readArray(), this._cs);
        return r2.readMap(t, pkg);
      }
      case Kind.Package:
        throw new Error('Not implemented');
      case Kind.Ref:
        throw new Error('Not implemented');
      case Kind.Set: {
        let r2 = new JsonArrayReader(this.readArray(), this._cs);
        return r2.readSet(t, pkg);
      }
      case Kind.Enum:
      case Kind.Struct:
        throw new Error('Not allowed');
      case Kind.TypeRef:
      case Kind.Unresolved:
        return this.readUnresolvedKindToValue(t, pkg);
    }

    throw new Error('Unreached');
  }

  readUnresolvedKindToValue(t: TypeRef, pkg: ?Package = null): any {
    let pkgRef = t.packageRef;
    let ordinal = t.ordinal;
    if (!pkgRef.isEmpty()) {
      let pkg2 = lookupPackage(pkgRef);
      if (!pkg2) {
        throw new Error('Not implemented');
      } else {
        pkg = pkg2;
      }
    }

    if (pkg) {
      let typeDef = pkg.types[ordinal];
      if (typeDef.kind === Kind.Enum) {
        throw new Error('Not implemented');
      }

      if (typeDef.kind !== Kind.Struct) {
        throw new Error('Attempt to resolve non-struct struct kind');
      }

      return this.readStruct(typeDef, t, pkg);
    } else {
      throw new Error('Woah, got a null pkg. pkgRef: ' + pkgRef.toString() + ' ordinal: ' + ordinal);
    }
  }

  readStruct(typeDef: TypeRef, typeRef: TypeRef, pkg: Package): any {
    // TODO FixupTypeRef?
    let desc = typeDef.desc;
    if (desc instanceof StructDesc) {
      let s = Object.create(null);

      for (let i = 0; i < desc.fields.length; i++) {
        let field = desc.fields[i];

        if (field.optional) {
          let b = this.readBool();
          if (b) {
            let v = this.readValueWithoutTag(field.t, pkg);
            s[field.name] = v;
          }
        } else {
          let v = this.readValueWithoutTag(field.t, pkg);
          s[field.name] = v;
        }
      }

      if (desc.union.length > 0) {
        throw new Error('Not implemented');
      }

      return s;
    } else {
      throw new Error('Attempt to read struct without StructDesc typeref');
    }
  }
}

function decodeNomsValue(chunk: Chunk): any {
  let tag = new Chunk(new Uint8Array(chunk.data.buffer, 0, 2)).toString();

  switch (tag) {
    case typedTag: {
      let ms = new MemoryStore(); // This needs to be handed in.
      let payload = JSON.parse(new Chunk(new Uint8Array(chunk.data.buffer, 2)).toString());
      let reader = new JsonArrayReader(payload, ms);
      return reader.readTopLevelValue();
    }
    default:
      throw new Error('Not implemented');
  }
}

export {decodeNomsValue, JsonArrayReader};
