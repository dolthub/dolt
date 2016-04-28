// @flow

import {NomsBlob, BlobLeafSequence} from './blob.js';
import Chunk from './chunk.js';
import Ref from './ref.js';
import RefValue from './ref-value.js';
import {newStruct} from './struct.js';
import type Struct from './struct.js';
import type DataStore from './data-store.js';
import type {NomsKind} from './noms-kind.js';
import {decode as decodeBase64} from './base64.js';
import {
  Field,
  getPrimitiveType,
  makeCompoundType,
  makeStructType,
  StructDesc,
  Type,
  typeType,
  numberType,
} from './type.js';
import {indexTypeForMetaSequence, MetaTuple, newMetaSequenceFromData} from './meta-sequence.js';
import {invariant} from './assert.js';
import {isPrimitiveKind, Kind} from './noms-kind.js';
import {ListLeafSequence, NomsList} from './list.js';
import {NomsMap, MapLeafSequence} from './map.js';
import {NomsSet, SetLeafSequence} from './set.js';
import {IndexedMetaSequence} from './meta-sequence.js';

const typedTag = 't ';
const blobTag = 'b ';

export class JsonArrayReader {
  _a: Array<any>;
  _i: number;
  _ds: DataStore;

  constructor(a: Array<any>, ds: DataStore) {
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

  readFloat(): number {
    const next = this.read();
    invariant(typeof next === 'string');
    return parseFloat(next);
  }

  readOrdinal(): number {
    return this.readInt();
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

  readTypeAsTag(backRefs: Type[]): Type {
    const kind = this.readKind();
    switch (kind) {
      case Kind.List:
      case Kind.Set:
      case Kind.Ref: {
        const elemType = this.readTypeAsTag(backRefs);
        return makeCompoundType(kind, elemType);
      }
      case Kind.Map: {
        const keyType = this.readTypeAsTag(backRefs);
        const valueType = this.readTypeAsTag(backRefs);
        return makeCompoundType(kind, keyType, valueType);
      }
      case Kind.Type:
        return typeType;
      case Kind.Struct:
        return this.readStructType(backRefs);
      case Kind.BackRef: {
        const i = this.readUint8();
        return backRefs[backRefs.length - 1 - i];
      }
    }

    if (isPrimitiveKind(kind)) {
      return getPrimitiveType(kind);
    }

    throw new Error('Unreachable');
  }


  readBlobLeafSequence(): BlobLeafSequence {
    const bytes = decodeBase64(this.readString());
    return new BlobLeafSequence(this._ds, bytes);
  }

  readSequence(t: Type): Array<any> {
    const elemType = t.elemTypes[0];
    const list = [];
    while (!this.atEnd()) {
      const v = this.readValueWithoutTag(elemType);
      list.push(v);
    }

    return list;
  }

  readListLeafSequence(t: Type): ListLeafSequence {
    const seq = this.readSequence(t);
    return new ListLeafSequence(this._ds, t, seq);
  }

  readSetLeafSequence(t: Type): SetLeafSequence {
    const seq = this.readSequence(t);
    return new SetLeafSequence(this._ds, t, seq);
  }

  readMapLeafSequence(t: Type): MapLeafSequence {
    const keyType = t.elemTypes[0];
    const valueType = t.elemTypes[1];
    const entries = [];
    while (!this.atEnd()) {
      const k = this.readValueWithoutTag(keyType);
      const v = this.readValueWithoutTag(valueType);
      entries.push({key: k, value: v});
    }

    return new MapLeafSequence(this._ds, t, entries);
  }

  readMetaSequence(t: Type): any {
    const data: Array<MetaTuple> = [];
    const indexType = indexTypeForMetaSequence(t);
    while (!this.atEnd()) {
      const ref = this.readRef();
      const v = this.readValueWithoutTag(indexType);
      const numLeaves = this.readValueWithoutTag(numberType);
      data.push(new MetaTuple(ref, v, numLeaves));
    }

    return newMetaSequenceFromData(this._ds, t, data);
  }

  readRefValue(t: Type): RefValue {
    const ref = this.readRef();
    return new RefValue(ref, t);
  }

  readTopLevelValue(): Promise<any> {
    const t = this.readTypeAsTag([]);
    const v = this.readValueWithoutTag(t);
    return Promise.resolve(v);
  }

  readValueWithoutTag(t: Type): any {
    // TODO: Verify read values match tagged kinds.
    switch (t.kind) {
      case Kind.Blob: {
        const isMeta = this.readBool();
        let sequence;
        if (isMeta) {
          const r2 = new JsonArrayReader(this.readArray(), this._ds);
          sequence = r2.readMetaSequence(t);
          invariant(sequence instanceof IndexedMetaSequence);
        } else {
          sequence = this.readBlobLeafSequence();
        }
        return new NomsBlob(sequence);
      }
      case Kind.Bool:
        return this.readBool();
      case Kind.Number:
        return this.readFloat();
      case Kind.String:
        return this.readString();
      case Kind.Value: {
        const t2 = this.readTypeAsTag([]);
        return this.readValueWithoutTag(t2);
      }
      case Kind.List: {
        const isMeta = this.readBool();
        const r2 = new JsonArrayReader(this.readArray(), this._ds);
        const sequence = isMeta ?
            r2.readMetaSequence(t) :
            r2.readListLeafSequence(t);
        return new NomsList(t, sequence);
      }
      case Kind.Map: {
        const isMeta = this.readBool();
        const r2 = new JsonArrayReader(this.readArray(), this._ds);
        const sequence = isMeta ?
          r2.readMetaSequence(t) :
          r2.readMapLeafSequence(t);
        return new NomsMap(t, sequence);
      }
      case Kind.Ref:
        return this.readRefValue(t);
      case Kind.Set: {
        const isMeta = this.readBool();
        const r2 = new JsonArrayReader(this.readArray(), this._ds);
        const sequence = isMeta ?
          r2.readMetaSequence(t) :
          r2.readSetLeafSequence(t);
        return new NomsSet(t, sequence);
      }
      case Kind.Struct:
        return this.readStruct(t);
      case Kind.Type:
        return this.readTypeAsValue([]);
    }

    throw new Error('Unreached');
  }

  readTypeAsValue(backRefs: Type[]): Type {
    const k = this.readKind();

    switch (k) {
      case Kind.List:
      case Kind.Map:
      case Kind.Ref:
      case Kind.Set: {
        const r2 = new JsonArrayReader(this.readArray(), this._ds);
        const elemTypes: Array<Type> = [];
        while (!r2.atEnd()) {
          elemTypes.push(r2.readTypeAsValue(backRefs));
        }

        return makeCompoundType(k, ...elemTypes);
      }
      case Kind.Struct:
        return this.readStructType(backRefs);

      case Kind.BackRef:
        throw new Error('not reachable');
    }

    invariant(isPrimitiveKind(k));
    return getPrimitiveType(k);
  }

  readStruct<T: Struct>(type: Type): T {
    const desc = type.desc;
    invariant(desc instanceof StructDesc);

    const data: {[key: string]: any} = Object.create(null);

    for (let i = 0; i < desc.fields.length; i++) {
      const field = desc.fields[i];
      if (field.optional) {
        const b = this.readBool();
        if (b) {
          const v = this.readValueWithoutTag(field.t);
          data[field.name] = v;
        }
      } else {
        const v = this.readValueWithoutTag(field.t);
        data[field.name] = v;
      }
    }

    return newStruct(type, data);
  }

  readStructType(backRefs: Type[]): Type {
    const name = this.readString();
    const fields = [];
    const structType = makeStructType(name, fields);
    backRefs.push(structType);

    const newFields: Array<Field> = [];
    const fieldReader = new JsonArrayReader(this.readArray(), this._ds);
    while (!fieldReader.atEnd()) {
      const fieldName = fieldReader.readString();
      const fieldType = fieldReader.readTypeAsTag(backRefs);
      const optional = fieldReader.readBool();
      newFields.push(new Field(fieldName, fieldType, optional));
    }

    // Mutate the already created structType since when looking for the cycle we compare
    // by identity.
    invariant(structType.desc instanceof StructDesc);
    structType.desc.fields = newFields;
    backRefs.pop();
    return structType;
  }
}

export function decodeNomsValue(chunk: Chunk, ds: DataStore): Promise<any> {
  const tag = new Chunk(new Uint8Array(chunk.data.buffer, 0, 2)).toString();

  switch (tag) {
    case typedTag: {
      const payload = JSON.parse(new Chunk(new Uint8Array(chunk.data.buffer, 2)).toString());
      const reader = new JsonArrayReader(payload, ds);
      return reader.readTopLevelValue();
    }
    case blobTag:
      return Promise.resolve(
          new NomsBlob(new BlobLeafSequence(ds, new Uint8Array(chunk.data.buffer, 2))));
    default:
      throw new Error('Not implemented');
  }
}
