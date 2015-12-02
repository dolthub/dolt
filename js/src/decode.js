/* @flow */

import Chunk from './chunk.js';
import Ref from './ref.js';
import Struct from './struct.js';
import type {ChunkStore} from './chunk_store.js';
import type {NomsKind} from './noms_kind.js';
import {decode as decodeBase64} from './base64.js';
import {Field, makeCompoundType, makeEnumType, makePrimitiveType, makeStructType, makeType, makeUnresolvedType, StructDesc, Type} from './type.js';
import {invariant, notNull} from './assert.js';
import {isPrimitiveKind, Kind} from './noms_kind.js';
import {lookupPackage, Package, readPackage} from './package.js';

const typedTag = 't ';
const blobTag = 'b ';

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
    invariant(typeof next === 'string');
    return next;
  }

  readBool(): boolean {
    let next = this.read();
    invariant(typeof next === 'boolean');
    return next;
  }

  readNumber(): number {
    let next = this.read();
    invariant(typeof next === 'number');
    return next;
  }

  readOrdinal(): number {
    return this.readNumber();
  }

  readArray(): Array<any> {
    let next = this.read();
    invariant(Array.isArray(next));
    return next;
  }

  readKind(): NomsKind {
    return this.readNumber();
  }

  readRef(): Ref {
    let next = this.readString();
    return Ref.parse(next);
  }

  readTypeAsTag(): Type {
    let kind = this.readKind();
    switch (kind) {
      case Kind.List:
      case Kind.Set:
      case Kind.Ref: {
        let elemType = this.readTypeAsTag();
        return makeCompoundType(kind, elemType);
      }
      case Kind.Map: {
        let keyType = this.readTypeAsTag();
        let valueType = this.readTypeAsTag();
        return makeCompoundType(kind, keyType, valueType);
      }
      case Kind.Type:
        return makePrimitiveType(Kind.Type);
      case Kind.Unresolved: {
        let pkgRef = this.readRef();
        let ordinal = this.readOrdinal();
        return makeType(pkgRef, ordinal);
      }
    }

    if (isPrimitiveKind(kind)) {
      return makePrimitiveType(kind);
    }

    throw new Error('Unreachable');
  }

  readBlob(): Promise<ArrayBuffer> {
    let s = this.readString();
    return Promise.resolve(decodeBase64(s));
  }

  async readList(t: Type, pkg: ?Package): Promise<Array<any>> {
    let elemType = t.elemTypes[0];
    let list = [];
    while (!this.atEnd()) {
      let v = await this.readValueWithoutTag(elemType, pkg);
      list.push(v);
    }

    return list;
  }

  async readSet(t: Type, pkg: ?Package): Promise<Set> {
    let seq = await this.readList(t, pkg);
    return new Set(seq);
  }

  async readMap(t: Type, pkg: ?Package): Promise<Map> {
    let keyType = t.elemTypes[0];
    let valueType = t.elemTypes[1];
    let m = new Map();
    while (!this.atEnd()) {
      let k = await this.readValueWithoutTag(keyType, pkg);
      let v = await this.readValueWithoutTag(valueType, pkg);
      m.set(k, v);
    }

    return m;
  }

  readEnum(): number {
    return this.readNumber();
  }

  readPackage(t: Type, pkg: ?Package): Package {
    let r2 = new JsonArrayReader(this.readArray(), this._cs);
    let types = [];
    while (!r2.atEnd()) {
      types.push(r2.readTypeAsValue(pkg));
    }

    let r3 = new JsonArrayReader(this.readArray(), this._cs);
    let deps = [];
    while (!r3.atEnd()) {
      deps.push(r3.readRef());
    }

    return new Package(types, deps);
  }

  readTopLevelValue(): Promise<any> {
    let t = this.readTypeAsTag();
    return this.readValueWithoutTag(t);
  }

  readValueWithoutTag(t: Type, pkg: ?Package = null): Promise<any> {
    // TODO: Verify read values match tagged kinds.
    switch (t.kind) {
      case Kind.Blob:
        return this.readBlob();
      case Kind.Bool:
        return Promise.resolve(this.readBool());
      case Kind.Uint8:
      case Kind.Uint16:
      case Kind.Uint32:
      case Kind.Uint64:
      case Kind.Int8:
      case Kind.Int16:
      case Kind.Int32:
      case Kind.Int64:
      case Kind.Float32:
      case Kind.Float64:
        return Promise.resolve(this.read());
      case Kind.String:
        return Promise.resolve(this.readString());
      case Kind.Value: {
        let t2 = this.readTypeAsTag();
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
        return Promise.resolve(this.readPackage(t, pkg));
      case Kind.Ref:
        return Promise.resolve(this.readRef());
      case Kind.Set: {
        let r2 = new JsonArrayReader(this.readArray(), this._cs);
        return r2.readSet(t, pkg);
      }
      case Kind.Enum:
      case Kind.Struct:
        throw new Error('Not allowed');
      case Kind.Type:
        return Promise.resolve(this.readTypeAsValue(pkg));
      case Kind.Unresolved:
        return this.readUnresolvedKindToValue(t, pkg);
    }

    throw new Error('Unreached');
  }

  async readUnresolvedKindToValue(t: Type, pkg: ?Package = null): Promise<any> {
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

    pkg = notNull(pkg);
    let typeDef = pkg.types[ordinal];
    if (typeDef.kind === Kind.Enum) {
      return this.readEnum();
    }

    invariant(typeDef.kind === Kind.Struct);
    return this.readStruct(typeDef, t, pkg);
  }

  readTypeAsValue(pkg: ?Package): Type {
    let k = this.readKind();

    switch (k) {
      case Kind.Enum:
        let name = this.readString();
        let r2 = new JsonArrayReader(this.readArray(), this._cs);
        let ids = [];
        while (!r2.atEnd()) {
          ids.push(r2.readString());
        }
        return makeEnumType(name, ids);
      case Kind.List:
      case Kind.Map:
      case Kind.Ref:
      case Kind.Set: {
        let r2 = new JsonArrayReader(this.readArray(), this._cs);
        let elemTypes: Array<Type> = [];
        while (!r2.atEnd()) {
          elemTypes.push(r2.readTypeAsValue());
        }

        return makeCompoundType(k, ...elemTypes);
      }
      case Kind.Struct: {
        let name = this.readString();
        let readFields = () => {
          let fields: Array<Field> = [];
          let fieldReader = new JsonArrayReader(this.readArray(), this._cs);
          while (!fieldReader.atEnd()) {
            let fieldName = fieldReader.readString();
            let fieldType = fieldReader.readTypeAsValue(pkg);
            let optional = fieldReader.readBool();
            fields.push(new Field(fieldName, fieldType, optional));
          }
          return fields;
        };

        let fields = readFields();
        let choices = readFields();
        return makeStructType(name, fields, choices);
      }
      case Kind.Unresolved: {
        let pkgRef = this.readRef();
        let ordinal = this.readOrdinal();
        if (ordinal === -1) {
          let namespace = this.readString();
          let name = this.readString();
          return makeUnresolvedType(namespace, name);
        }

        return makeType(pkgRef, ordinal);
      }
    }

    invariant(isPrimitiveKind(k));
    return makePrimitiveType(k);

  }

  async readStruct(typeDef: Type, type: Type, pkg: Package): Promise<Struct> {
    // TODO FixupType?
    let desc = typeDef.desc;
    invariant(desc instanceof StructDesc);

    let s: { [key: string]: any } = Object.create(null);

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

    let unionIndex = -1;
    if (desc.union.length > 0) {
      unionIndex = this.readNumber();
      let unionField = desc.union[unionIndex];
      let v = await this.readValueWithoutTag(unionField.t, pkg);
      s[unionField.name] = v;
    }

    return new Struct(type, typeDef, s);
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
    case blobTag: {
      return Promise.resolve(chunk.data.buffer.slice(2));
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
