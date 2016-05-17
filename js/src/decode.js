// @flow

import {NomsBlob, BlobLeafSequence} from './blob.js';
import Chunk from './chunk.js';
import Ref from './ref.js';
import {default as RefValue, constructRefValue} from './ref-value.js';
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
import {ListLeafSequence, NomsList} from './list.js';
import {NomsMap, MapLeafSequence} from './map.js';
import {NomsSet, SetLeafSequence} from './set.js';
import {IndexedMetaSequence, OrderedMetaSequence} from './meta-sequence.js';
import type {valueOrPrimitive} from './value.js';
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

  readRef(): Ref {
    const next = this.readString();
    return Ref.parse(next);
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

  readListLeafSequence(): ListLeafSequence {
    const seq = this.readSequence();
    // TODO(arv): Pass along type here so we do not have to compute it again.
    return new ListLeafSequence(this._ds, seq);
  }

  readSetLeafSequence(): SetLeafSequence {
    const seq = this.readSequence();
    // TODO(arv): Pass along type here so we do not have to compute it again.
    return new SetLeafSequence(this._ds, seq);
  }

  readMapLeafSequence(): MapLeafSequence {
    const entries = [];
    while (!this.atEnd()) {
      const k = this.readValue();
      const v = this.readValue();
      entries.push({key: k, value: v});
    }

    // TODO(arv): Pass along type here so we do not have to compute it again.
    return new MapLeafSequence(this._ds, entries);
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

  readRefValue(t: Type): RefValue {
    const ref = this.readRef();
    const height = this.readInt();
    return constructRefValue(t, ref, height);
  }

  readValue(): any {
    const t = this.readType([]);
    switch (t.kind) {
      case Kind.Blob: {
        const isMeta = this.readBool();
        if (isMeta) {
          const r2 = new JsonArrayReader(this.readArray(), this._ds);
          return new NomsBlob(r2.readIndexedMetaSequence(t));
        }

        return new NomsBlob(this.readBlobLeafSequence());
      }
      case Kind.Bool:
        return this.readBool();
      case Kind.Number:
        return this.readFloat();
      case Kind.String:
        return this.readString();
      case Kind.Value: {
        return this.readValue();
      }
      case Kind.List: {
        const isMeta = this.readBool();
        const r2 = new JsonArrayReader(this.readArray(), this._ds);
        if (isMeta) {
          return new NomsList(r2.readIndexedMetaSequence(t));
        }
        return new NomsList(r2.readListLeafSequence());
      }
      case Kind.Map: {
        const isMeta = this.readBool();
        const r2 = new JsonArrayReader(this.readArray(), this._ds);
        if (isMeta) {
          return new NomsMap(r2.readOrderedMetaSequence(t));
        }
        return new NomsMap(r2.readMapLeafSequence());
      }
      case Kind.Ref:
        return this.readRefValue(t);
      case Kind.Set: {
        const isMeta = this.readBool();
        const r2 = new JsonArrayReader(this.readArray(), this._ds);
        if (isMeta) {
          return new NomsSet(r2.readOrderedMetaSequence(t));
        }
        return new NomsSet(r2.readSetLeafSequence());
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

    const data: {[key: string]: valueOrPrimitive} = Object.create(null);

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

export function decodeNomsValue(chunk: Chunk, vr: ValueReader): valueOrPrimitive {
  const tag = new Chunk(new Uint8Array(chunk.data.buffer, 0, 2)).toString();

  switch (tag) {
    case typedTag: {
      const payload = JSON.parse(new Chunk(new Uint8Array(chunk.data.buffer, 2)).toString());
      const reader = new JsonArrayReader(payload, vr);
      return reader.readValue();
    }
    case blobTag:
      return new NomsBlob(new BlobLeafSequence(vr, new Uint8Array(chunk.data.buffer, 2)));
    default:
      throw new Error('Not implemented');
  }
}
