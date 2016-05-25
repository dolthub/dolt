// @flow

import Blob, {BlobLeafSequence} from './blob.js';
import Chunk from './chunk.js';
import Hash from './hash.js';
import Ref, {constructRef} from './ref.js';
import {newStructWithTypeNoValidation} from './struct.js';
import type Struct from './struct.js';
import type {NomsKind} from './noms-kind.js';
import {decode as decodeBase64} from './base64.js';
import {
  getPrimitiveType,
  makeListType,
  makeMapType,
  makeRefType,
  makeSetType,
  makeStructType,
  makeUnionType,
  StructDesc,
  Type,
  typeType,
} from './type.js';
import {MetaTuple} from './meta-sequence.js';
import {invariant} from './assert.js';
import {isPrimitiveKind, Kind} from './noms-kind.js';
import List, {ListLeafSequence} from './list.js';
import Map, {MapLeafSequence} from './map.js';
import Set, {SetLeafSequence} from './set.js';
import {IndexedMetaSequence, OrderedMetaSequence} from './meta-sequence.js';
import {ValueBase, setHash} from './value.js';
import type Value from './value.js';
import type {ValueReader} from './value-store.js';

const typedTag = 't ';
const blobTag = 'b ';

export class JsonArrayReader {
  _a: Array<any>;
  _i: number;
  _ds: ValueReader;

  constructor(a: Array<any>, ds: ValueReader) {
    this._a = a;
    this._i = 0;
    this._ds = ds;
  }

  read(): any {
    return this._a[this._i++];
  }

  atEnd(): boolean {
    return this._i >= this._a.length;
  }

  readString(): string {
    const next = this.read();
    invariant(typeof next === 'string');
    return next;
  }

  readBool(): boolean {
    const next = this.read();
    invariant(typeof next === 'boolean');
    return next;
  }

  readInt(): number {
    const next = this.read();
    invariant(typeof next === 'string');
    return parseInt(next, 10);
  }

  readUint(): number {
    const v = this.readInt();
    invariant(v >= 0);
    return v;
  }

  readUint8(): number {
    const v = this.read();
    invariant((v & 0xff) === v);
    return v;
  }

  readUint16(): number {
    const v = this.read();
    invariant((v & 0xffff) === v);
    return v;
  }

  readFloat(): number {
    const next = this.read();
    invariant(typeof next === 'string');
    return parseFloat(next);
  }

  readArray(): Array<any> {
    const next = this.read();
    invariant(Array.isArray(next));
    return next;
  }

  readKind(): NomsKind {
    const next = this.read();
    invariant(typeof next === 'number');
    return next;
  }

  readHash(): Hash {
    const next = this.readString();
    return Hash.parse(next);
  }

  readType(parentStructTypes: Type[]): Type {
    const k = this.readKind();
    switch (k) {
      case Kind.List:
        return makeListType(this.readType(parentStructTypes));
      case Kind.Map:
        return makeMapType(this.readType(parentStructTypes),
                           this.readType(parentStructTypes));
      case Kind.Set:
        return makeSetType(this.readType(parentStructTypes));
      case Kind.Ref:
        return makeRefType(this.readType(parentStructTypes));

      case Kind.Union: {
        const len = this.readUint16();
        const types: Type[] = new Array(len);
        for (let i = 0; i < len; i++) {
          types[i] = this.readType(parentStructTypes);
        }
        return makeUnionType(types);
      }
      case Kind.Type:
        return typeType;
      case Kind.Struct:
        return this.readStructType(parentStructTypes);
      case Kind.Parent: {
        const i = this.readUint8();
        return parentStructTypes[parentStructTypes.length - 1 - i];
      }
    }

    invariant(isPrimitiveKind(k));
    return getPrimitiveType(k);
  }

  readBlobLeafSequence(): BlobLeafSequence {
    const bytes = decodeBase64(this.readString());
    return new BlobLeafSequence(this._ds, bytes);
  }

  readSequence(): Array<any> {
    const list = [];
    while (!this.atEnd()) {
      const v = this.readValue();
      list.push(v);
    }

    return list;
  }

  readListLeafSequence(t: Type): ListLeafSequence {
    const seq = this.readSequence();
    return new ListLeafSequence(this._ds, t, seq);
  }

  readSetLeafSequence(t: Type): SetLeafSequence {
    const seq = this.readSequence();
    return new SetLeafSequence(this._ds, t, seq);
  }

  readMapLeafSequence(t: Type): MapLeafSequence {
    const entries = [];
    while (!this.atEnd()) {
      const k = this.readValue();
      const v = this.readValue();
      entries.push([k, v]);
    }

    return new MapLeafSequence(this._ds, t, entries);
  }

  readMetaSequence(): Array<MetaTuple> {
    const data: Array<MetaTuple> = [];
    while (!this.atEnd()) {
      const ref = this.readValue();
      const v = this.readValue();
      const numLeaves = this.readInt();
      data.push(new MetaTuple(ref, v, numLeaves));
    }

    return data;
  }

  readIndexedMetaSequence(t: Type): IndexedMetaSequence {
    return new IndexedMetaSequence(this._ds, t, this.readMetaSequence());
  }

  readOrderedMetaSequence(t: Type): OrderedMetaSequence {
    return new OrderedMetaSequence(this._ds, t, this.readMetaSequence());
  }

  readRef(t: Type): Ref {
    const hash = this.readHash();
    const height = this.readInt();
    return constructRef(t, hash, height);
  }

  readValue(): any {
    const t = this.readType([]);
    switch (t.kind) {
      case Kind.Blob: {
        const isMeta = this.readBool();
        if (isMeta) {
          const r2 = new JsonArrayReader(this.readArray(), this._ds);
          return Blob.fromSequence(r2.readIndexedMetaSequence(t));
        }

        return Blob.fromSequence(this.readBlobLeafSequence());
      }
      case Kind.Bool:
        return this.readBool();
      case Kind.Number:
        return this.readFloat();
      case Kind.String:
        return this.readString();
      case Kind.List: {
        const isMeta = this.readBool();
        const r2 = new JsonArrayReader(this.readArray(), this._ds);
        if (isMeta) {
          return List.fromSequence(r2.readIndexedMetaSequence(t));
        }
        return List.fromSequence(r2.readListLeafSequence(t));
      }
      case Kind.Map: {
        const isMeta = this.readBool();
        const r2 = new JsonArrayReader(this.readArray(), this._ds);
        if (isMeta) {
          return Map.fromSequence(r2.readOrderedMetaSequence(t));
        }
        return Map.fromSequence(r2.readMapLeafSequence(t));
      }
      case Kind.Ref:
        return this.readRef(t);
      case Kind.Set: {
        const isMeta = this.readBool();
        const r2 = new JsonArrayReader(this.readArray(), this._ds);
        if (isMeta) {
          return Set.fromSequence(r2.readOrderedMetaSequence(t));
        }
        return Set.fromSequence(r2.readSetLeafSequence(t));
      }
      case Kind.Struct:
        return this.readStruct(t);
      case Kind.Type:
        return this.readType([]);
    }

    throw new Error('Unreached');
  }

  readStruct<T: Struct>(type: Type): T {
    const {desc} = type;
    invariant(desc instanceof StructDesc);

    const data: {[key: string]: Value} = Object.create(null);

    desc.forEachField((name: string) => {
      const v = this.readValue();
      data[name] = v;
    });

    return newStructWithTypeNoValidation(type, data);
  }

  readStructType(parentStructTypes: Type[]): Type {
    const name = this.readString();
    const fields = {};
    const structType = makeStructType(name, fields);
    parentStructTypes.push(structType);

    const newFields = Object.create(null);
    const fieldReader = new JsonArrayReader(this.readArray(), this._ds);
    while (!fieldReader.atEnd()) {
      const fieldName = fieldReader.readString();
      const fieldType = fieldReader.readType(parentStructTypes);
      newFields[fieldName] = fieldType;
    }

    // Mutate the already created structType since when looking for the cycle we compare
    // by identity.
    structType.desc.fields = newFields;
    parentStructTypes.pop();
    return structType;
  }
}

export function decodeNomsValue(chunk: Chunk, vr: ValueReader): Value {
  const tag = new Chunk(new Uint8Array(chunk.data.buffer, 0, 2)).toString();
  let v: Value;
  switch (tag) {
    case typedTag: {
      const payload = JSON.parse(new Chunk(new Uint8Array(chunk.data.buffer, 2)).toString());
      const reader = new JsonArrayReader(payload, vr);
      v = reader.readValue();
      break;
    }
    case blobTag:
      v = Blob.fromSequence(new BlobLeafSequence(vr, new Uint8Array(chunk.data.buffer, 2)));
      break;
    default:
      throw new Error('Not implemented');
  }
  if (v instanceof ValueBase) {
    setHash(v, chunk.hash);
  }
  return v;
}
