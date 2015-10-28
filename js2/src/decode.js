/* @flow */

'use strict';

import type {ChunkStore} from './chunk_store.js';
import type {NomsKind} from './noms_kind.js';

const {isPrimitiveKind, Kind} = require('./noms_kind.js');
const Ref = require('./ref.js');
const {makeCompoundTypeRef, makePrimitiveTypeRef, makeTypeRef, TypeRef} = require('./type_ref.js');

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

  readList(t: TypeRef, pkg: ?Ref): Array<any> {
    let elemType = t.elemTypes[0];
    let list = [];
    while (!this.atEnd()) {
      let v = this.readValueWithoutTag(elemType, pkg);
      list.push(v);
    }
    return list;
  }

  readSet(t: TypeRef, pkg: ?Ref): Set {
    let elemType = t.elemTypes[0];
    let s = new Set();
    while (!this.atEnd()) {
      let v = this.readValueWithoutTag(elemType, pkg);
      s.add(v);
    }

    return s;
  }

  readMap(t: TypeRef, pkg: ?Ref): Map {
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

  readValueWithoutTag(t: TypeRef, pkg: ?Ref = null): any {
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
      throw new Error('Not implemented');
    }

    throw new Error('Unreached');
  }
}

module.exports = {JsonArrayReader};
