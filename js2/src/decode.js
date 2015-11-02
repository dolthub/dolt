/* @flow */

'use strict';

import Chunk from './chunk.js';
import Ref from './ref.js';
import type {ChunkStore} from './chunk_store.js';
import type {NomsKind} from './noms_kind.js';
import {isPrimitiveKind, Kind} from './noms_kind.js';
import {lookupPackage, Package, readPackage} from './package.js';
import {Field, makeCompoundTypeRef, makePrimitiveTypeRef, makeStructTypeRef, makeTypeRef, makeUnresolvedTypeRef, StructDesc, TypeRef} from './type_ref.js';

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

  async readList(t: TypeRef, pkg: ?Package): Promise<Array<any>> {
    let elemType = t.elemTypes[0];
    let list = [];
    while (!this.atEnd()) {
      let v = this.readValueWithoutTag(elemType, pkg);
      list.push(v);
    }

    return Promise.all(list);
  }

  async readSet(t: TypeRef, pkg: ?Package): Promise<Set> {
    let seq = await this.readList(t, pkg);
    return new Set(seq);
  }

  async readMap(t: TypeRef, pkg: ?Package): Promise<Map> {
    let keyType = t.elemTypes[0];
    let valueType = t.elemTypes[1];
    let kv = [];
    while (!this.atEnd()) {
      kv.push(this.readValueWithoutTag(keyType, pkg));
      kv.push(this.readValueWithoutTag(valueType, pkg));
    }

    kv = await Promise.all(kv);
    let m = new Map();
    for (let i = 0; i < kv.length; i += 2) {
      m.set(kv[i], kv[i + 1]);
    }

    return m;
  }

  readPackage(t: TypeRef, pkg: ?Package): Package {
    let r2 = new JsonArrayReader(this.readArray(), this._cs);
    let types = [];
    while (!r2.atEnd()) {
      types.push(r2.readTypeRefAsValue(pkg));
    }

    let r3 = new JsonArrayReader(this.readArray(), this._cs);
    let deps = [];
    while (!r3.atEnd()) {
      deps.push(r3.readRef());
    }

    return new Package(types, deps);
  }

  async readTopLevelValue(): Promise<any> {
    let t = this.readTypeRefAsTag();
    return this.readValueWithoutTag(t);
  }

  async readValueWithoutTag(t: TypeRef, pkg: ?Package = null): Promise<any> {
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
        return this.readPackage(t, pkg);
      case Kind.Ref:
        return this.readRef();
      case Kind.Set: {
        let r2 = new JsonArrayReader(this.readArray(), this._cs);
        return r2.readSet(t, pkg);
      }
      case Kind.Enum:
      case Kind.Struct:
        throw new Error('Not allowed');
      case Kind.TypeRef:
        return this.readTypeRefAsValue(pkg);
      case Kind.Unresolved:
        return this.readUnresolvedKindToValue(t, pkg);
    }

    throw new Error('Unreached');
  }

  async readUnresolvedKindToValue(t: TypeRef, pkg: ?Package = null): Promise<any> {
    let pkgRef = t.packageRef;
    let ordinal = t.ordinal;
    if (!pkgRef.isEmpty()) {
      let pkg2 = lookupPackage(pkgRef);
      if (!pkg2) {
        pkg = await readPackage(pkgRef, this._cs);
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

  readTypeRefAsValue(pkg: ?Package): TypeRef {
    let k = this.readKind();

    switch (k) {
      case Kind.Enum:
        throw new Error('Not implemented');
      case Kind.List:
      case Kind.Map:
      case Kind.Ref:
      case Kind.Set: {
        let r2 = new JsonArrayReader(this.readArray(), this._cs);
        let elemTypes: Array<TypeRef> = [];
        while (!r2.atEnd()) {
          elemTypes.push(r2.readTypeRefAsValue());
        }

        return makeCompoundTypeRef(k, ...elemTypes);
      }
      case Kind.Struct: {
        let name = this.readString();
        let readFields = () => {
          let fields: Array<Field> = [];
          let fieldReader = new JsonArrayReader(this.readArray(), this._cs);
          while (!fieldReader.atEnd()) {
            let fieldName = fieldReader.readString();
            let fieldType = fieldReader.readTypeRefAsValue(pkg);
            let optional = fieldReader.readBool();
            fields.push(new Field(fieldName, fieldType, optional));
          }
          return fields;
        };

        let fields = readFields();
        let choices = readFields();
        return makeStructTypeRef(name, fields, choices);
      }
      case Kind.Unresolved: {
        let pkgRef = this.readRef();
        let ordinal = this.readOrdinal();
        if (ordinal === -1) {
          let namespace = this.readString();
          let name = this.readString();
          if (!pkgRef.isEmpty()) {
            throw new Error('Unresolved TypeRefs may not have a package ref');
          }

          return makeUnresolvedTypeRef(namespace, name);
        }

        return makeTypeRef(pkgRef, ordinal);
      }
      default: {
        if (!isPrimitiveKind(k)) {
          throw new Error('Not implemented: ' + k);
        }
        return makePrimitiveTypeRef(k);
      }
    }
  }

  async readStruct(typeDef: TypeRef, typeRef: TypeRef, pkg: Package): Promise<any> {
    // TODO FixupTypeRef?
    // TODO Make read of sub-values parallel.
    let desc = typeDef.desc;
    if (desc instanceof StructDesc) {
      let s: { [key: string]: any } = Object.create(null);
      s._typeRef = typeDef;  // TODO: Need a better way to add typeRef

      for (let field of desc.fields) {
        if (field.optional) {
          let b = this.readBool();
          if (b) {
            let v = await this.readValueWithoutTag(field.t, pkg);
            s[field.name] = v;
          }
        } else {
          let v = await this.readValueWithoutTag(field.t, pkg);
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

function decodeNomsValue(chunk: Chunk, cs: ChunkStore): Promise<any> {
  let tag = new Chunk(new Uint8Array(chunk.data.buffer, 0, 2)).toString();

  switch (tag) {
    case typedTag: {
      let payload = JSON.parse(new Chunk(new Uint8Array(chunk.data.buffer, 2)).toString());
      let reader = new JsonArrayReader(payload, cs);
      return reader.readTopLevelValue();
    }
    default:
      throw new Error('Not implemented');
  }
}

export async function readValue(r: Ref, cs: ChunkStore): Promise<any> {
  let chunk = await cs.get(r);
  if (chunk.isEmpty()) {
    return null;
  }

  return decodeNomsValue(chunk, cs);
}

export {decodeNomsValue, JsonArrayReader, readValue};
