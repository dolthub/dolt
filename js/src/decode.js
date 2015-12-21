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
import {ListLeafSequence, NomsList} from './list.js';
import {lookupPackage, Package, readPackage} from './package.js';
import {NomsMap, MapLeafSequence} from './map.js';
import {setDecodeNomsValue} from './read_value.js';
import {NomsSet, SetLeafSequence} from './set.js';

const typedTag = 't ';
const blobTag = 'b ';

class UnresolvedPackage {
  pkgRef: Ref;

  constructor(pkgRef: Ref) {
    this.pkgRef = pkgRef;
  }
}

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

  readSequence(t: Type, pkg: ?Package): Array<any> {
    let elemType = t.elemTypes[0];
    let list = [];
    while (!this.atEnd()) {
      let v = this.readValueWithoutTag(elemType, pkg);
      list.push(v);
    }

    return list;
  }

  readListLeafSequence(t: Type, pkg: ?Package): ListLeafSequence {
    let seq = this.readSequence(t, pkg);
    return new ListLeafSequence(t, seq);
  }

  readSetLeafSequence(t: Type, pkg: ?Package): SetLeafSequence {
    let seq = this.readSequence(t, pkg);
    return new SetLeafSequence(t, seq);
  }

  readMapLeafSequence(t: Type, pkg: ?Package): MapLeafSequence {
    let keyType = t.elemTypes[0];
    let valueType = t.elemTypes[1];
    let entries = [];
    while (!this.atEnd()) {
      let k = this.readValueWithoutTag(keyType, pkg);
      let v = this.readValueWithoutTag(valueType, pkg);
      entries.push({key: k, value: v});
    }

    return new MapLeafSequence(t, entries);
  }

  readEnum(): number {
    return this.readUint();
  }

  readMetaSequence(t: Type, pkg: ?Package): any {
    let data: Array<MetaTuple> = [];
    let indexType = indexTypeForMetaSequence(t);
    while (!this.atEnd()) {
      let ref = this.readRef();
      let v = this.readValueWithoutTag(indexType, pkg);
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
    return new Promise((resolve, reject) => {
      let t = this.readTypeAsTag();
      let doRead = () => {
        let i = this._i;

        try {
          let v = this.readValueWithoutTag(t);
          resolve(v);
        } catch (ex) {
          if (ex instanceof UnresolvedPackage) {
            readPackage(ex.pkgRef, this._cs).then(() => {
              this._i = i;
              doRead();
            });
          } else {
            reject(ex);
          }
        }
      };

      doRead();
    });
  }

  readValueWithoutTag(t: Type, pkg: ?Package = null): any {
    // TODO: Verify read values match tagged kinds.
    switch (t.kind) {
      case Kind.Blob:
        let isMeta = this.readBool();
        // https://github.com/attic-labs/noms/issues/798
        invariant(!isMeta, 'CompoundBlob not supported');
        return this.readBlob();

      case Kind.Bool:
        return this.readBool();
      case Kind.Float32:
      case Kind.Float64:
        return this.readFloat();
      case Kind.Int8:
      case Kind.Int16:
      case Kind.Int32:
      case Kind.Int64:
        return this.readInt();
      case Kind.Uint8:
      case Kind.Uint16:
      case Kind.Uint32:
      case Kind.Uint64:
        return this.readUint();
      case Kind.String:
        return this.readString();
      case Kind.Value: {
        let t2 = this.readTypeAsTag();
        return this.readValueWithoutTag(t2, pkg);
      }
      case Kind.List: {
        let isMeta = this.readBool();
        let r2 = new JsonArrayReader(this.readArray(), this._cs);
        let sequence = isMeta ?
            r2.readMetaSequence(t, pkg) :
            r2.readListLeafSequence(t, pkg);
        return new NomsList(this._cs, t, sequence);
      }
      case Kind.Map: {
        let isMeta = this.readBool();
        let r2 = new JsonArrayReader(this.readArray(), this._cs);
        let sequence = isMeta ?
          r2.readMetaSequence(t, pkg) :
          r2.readMapLeafSequence(t, pkg);
        return new NomsMap(this._cs, t, sequence);
      }
      case Kind.Package:
        return this.readPackage(t, pkg);
      case Kind.Ref:
        // TODO: This is not aligned with Go. In Go we have a dedicated Value
        // for refs.
        return this.readRef();
      case Kind.Set: {
        let isMeta = this.readBool();
        let r2 = new JsonArrayReader(this.readArray(), this._cs);
        let sequence = isMeta ?
          r2.readMetaSequence(t, pkg) :
          r2.readSetLeafSequence(t, pkg);
        return new NomsSet(this._cs, t, sequence);
      }
      case Kind.Enum:
      case Kind.Struct:
        throw new Error('Not allowed');
      case Kind.Type:
        return this.readTypeAsValue(pkg);
      case Kind.Unresolved:
        return this.readUnresolvedKindToValue(t, pkg);
    }

    throw new Error('Unreached');
  }

  readUnresolvedKindToValue(t: Type, pkg: ?Package = null): any {
    let pkgRef = t.packageRef;
    let ordinal = t.ordinal;
    if (!pkgRef.isEmpty()) {
      pkg = lookupPackage(pkgRef);
      if (!pkg) {
        throw new UnresolvedPackage(pkgRef);
      }
      invariant(pkg);
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

  readStruct(typeDef: Type, type: Type, pkg: Package): Struct {
    // TODO FixupType?
    let desc = typeDef.desc;
    invariant(desc instanceof StructDesc);

    let s: { [key: string]: any } = Object.create(null);

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

    let unionIndex = -1;
    if (desc.union.length > 0) {
      unionIndex = this.readUint();
      let unionField = desc.union[unionIndex];
      let v = this.readValueWithoutTag(unionField.t, pkg);
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
