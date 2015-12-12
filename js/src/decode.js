// @flow

import Chunk from './chunk.js';
import Ref from './ref.js';
import Struct from './struct.js';
import type {ChunkStore} from './chunk_store.js';
import type {NomsKind} from './noms_kind.js';
import {decode as decodeBase64} from './base64.js';
import {Field, makeCompoundType, makeEnumType, makePrimitiveType, makeStructType, makeType, makeUnresolvedType, StructDesc, Type} from './type.js';
import {indexTypeForMetaSequence, MetaTuple, newMetaSequenceFromData} from './meta_sequence.js';
import {invariant, notNull} from './assert.js';
import {isPrimitiveKind, Kind} from './noms_kind.js';
import {ListLeaf} from './list.js';
import {lookupPackage, Package, readPackage} from './package.js';
import {MapLeaf} from './map.js';
import {setDecodeNomsValue} from './read_value.js';
import {SetLeaf} from './set.js';

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

  readInt(): number {
    let next = this.read();
    invariant(typeof next === 'string');
    return parseInt(next, 10);
  }

  readUint(): number {
    let v = this.readInt();
    invariant(v >= 0);
    return v;
  }

  readFloat(): number {
    let next = this.read();
    invariant(typeof next === 'string');
    return parseFloat(next);
  }

  readOrdinal(): number {
    return this.readInt();
  }

  readArray(): Array<any> {
    let next = this.read();
    invariant(Array.isArray(next));
    return next;
  }

  readKind(): NomsKind {
    let next = this.read();
    invariant(typeof next === 'number');
    return next;
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

  async readSequence(t: Type, pkg: ?Package): Promise<Array<any>> {
    let elemType = t.elemTypes[0];
    let list = [];
    while (!this.atEnd()) {
      let v = await this.readValueWithoutTag(elemType, pkg);
      list.push(v);
    }

    return list;
  }

  async readListLeaf(t: Type, pkg: ?Package): Promise<ListLeaf> {
    let seq = await this.readSequence(t, pkg);
    return new ListLeaf(this._cs, t, seq);
  }

  async readSetLeaf(t: Type, pkg: ?Package): Promise<SetLeaf> {
    let seq = await this.readSequence(t, pkg);
    return new SetLeaf(this._cs, t, seq);
  }

  async readMapLeaf(t: Type, pkg: ?Package): Promise<MapLeaf> {
    let keyType = t.elemTypes[0];
    let valueType = t.elemTypes[1];
    let entries = [];
    while (!this.atEnd()) {
      let k = await this.readValueWithoutTag(keyType, pkg);
      let v = await this.readValueWithoutTag(valueType, pkg);
      entries.push({key: k, value: v});
    }

    return new MapLeaf(this._cs, t, entries);
  }

  readEnum(): number {
    return this.readUint();
  }

  async maybeReadMetaSequence(t: Type, pkg: ?Package): Promise<any> {
    if (!this.readBool()) {
      return null;
    }

    let r2 = new JsonArrayReader(this.readArray(), this._cs);
    let data: Array<MetaTuple> = [];
    let indexType = indexTypeForMetaSequence(t);
    while (!r2.atEnd()) {
      let ref = r2.readRef();
      let v = await r2.readValueWithoutTag(indexType, pkg);
      data.push(new MetaTuple(ref, v));
    }

    return newMetaSequenceFromData(this._cs, t, data);
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

  async readValueWithoutTag(t: Type, pkg: ?Package = null): Promise<any> {
    // TODO: Verify read values match tagged kinds.
    switch (t.kind) {
      case Kind.Blob:
        let ms = await this.maybeReadMetaSequence(t, pkg);
        if (ms) {
          return ms;
        }

        return this.readBlob();

      case Kind.Bool:
        return Promise.resolve(this.readBool());
      case Kind.Float32:
      case Kind.Float64:
        return Promise.resolve(this.readFloat());
      case Kind.Int8:
      case Kind.Int16:
      case Kind.Int32:
      case Kind.Int64:
        return Promise.resolve(this.readInt());
      case Kind.Uint8:
      case Kind.Uint16:
      case Kind.Uint32:
      case Kind.Uint64:
        return Promise.resolve(this.readUint());
      case Kind.String:
        return Promise.resolve(this.readString());
      case Kind.Value: {
        let t2 = this.readTypeAsTag();
        return this.readValueWithoutTag(t2, pkg);
      }
      case Kind.List: {
        let ms = await this.maybeReadMetaSequence(t, pkg);
        if (ms) {
          return ms;
        }

        let r2 = new JsonArrayReader(this.readArray(), this._cs);
        return r2.readListLeaf(t, pkg);
      }
      case Kind.Map: {
        let ms = await this.maybeReadMetaSequence(t, pkg);
        if (ms) {
          return ms;
        }

        let r2 = new JsonArrayReader(this.readArray(), this._cs);
        return r2.readMapLeaf(t, pkg);
      }
      case Kind.Package:
        return Promise.resolve(this.readPackage(t, pkg));
      case Kind.Ref:
        // TODO: This is not aligned with Go. In Go we have a dedicated Value
        // for refs.
        return Promise.resolve(this.readRef());
      case Kind.Set: {
        let ms = await this.maybeReadMetaSequence(t, pkg);
        if (ms) {
          return ms;
        }

        let r2 = new JsonArrayReader(this.readArray(), this._cs);
        return r2.readSetLeaf(t, pkg);
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

    for (let i = 0; i < desc.fields.length; i++) {
      let field = desc.fields[i];
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
      unionIndex = this.readUint();
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

export {decodeNomsValue, indexTypeForMetaSequence, JsonArrayReader};

setDecodeNomsValue(decodeNomsValue); // TODO: Avoid cyclic badness with commonjs.
