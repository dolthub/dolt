// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import Blob, {BlobLeafSequence} from './blob.js';
import Ref, {constructRef} from './ref.js';
import {newStructWithValues} from './struct.js';
import type Struct from './struct.js';
import type {NomsKind} from './noms-kind.js';
import {
  getPrimitiveType,
  makeListType,
  makeMapType,
  makeRefType,
  makeSetType,
  makeUnionType,
  StructDesc,
  Type,
} from './type.js';
import {OrderedKey, MetaTuple} from './meta-sequence.js';
import {invariant} from './assert.js';
import {isPrimitiveKind, kindToString, Kind} from './noms-kind.js';
import List, {ListLeafSequence} from './list.js';
import Map, {MapLeafSequence} from './map.js';
import Set, {SetLeafSequence} from './set.js';
import {IndexedMetaSequence, OrderedMetaSequence} from './meta-sequence.js';
import type Value from './value.js';
import type {ValueReader} from './value-store.js';
import type {NomsReader} from './codec.js';

export default class ValueDecoder {
  _r: NomsReader;
  _ds: ValueReader;

  constructor(r: NomsReader, ds: ValueReader) {
    this._r = r;
    this._ds = ds;
  }

  readKind(): NomsKind {
    return this._r.readUint8();
  }

  readRef(t: Type): Ref {
    const hash = this._r.readHash();
    const height = this._r.readUint64();
    return constructRef(t, hash, height);
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
      case Kind.Struct:
        return this.readStructType(parentStructTypes);
      case Kind.Union: {
        const len = this._r.readUint32();
        const types: Type[] = new Array(len);
        for (let i = 0; i < len; i++) {
          types[i] = this.readType(parentStructTypes);
        }
        return makeUnionType(types);
      }
      case Kind.Cycle: {
        const i = this._r.readUint32();
        return parentStructTypes[parentStructTypes.length - 1 - i];
      }
    }

    invariant(isPrimitiveKind(k));
    return getPrimitiveType(k);
  }

  readBlobLeafSequence(): BlobLeafSequence {
    const bytes = this._r.readBytes();
    return new BlobLeafSequence(this._ds, bytes);
  }

  readValueSequence(): Array<Value> {
    const count = this._r.readUint32();
    const list = [];
    for (let i = 0; i < count; i++) {
      const v = this.readValue();
      list.push(v);
    }

    return list;
  }

  readListLeafSequence(t: Type): ListLeafSequence {
    const data = this.readValueSequence();
    return new ListLeafSequence(this._ds, t, data);
  }

  readSetLeafSequence(t: Type): SetLeafSequence {
    const data = this.readValueSequence();
    return new SetLeafSequence(this._ds, t, data);
  }

  readMapLeafSequence(t: Type): MapLeafSequence {
    const count = this._r.readUint32();
    const data = [];
    for (let i = 0; i < count; i++) {
      const k = this.readValue();
      const v = this.readValue();
      data.push([k, v]);
    }

    return new MapLeafSequence(this._ds, t, data);
  }

  readMetaSequence(): Array<MetaTuple> {
    const count = this._r.readUint32();

    const data: Array<MetaTuple> = [];
    for (let i = 0; i < count; i++) {
      const ref = this.readValue();
      const v = this.readValue();
      const key = v instanceof Ref ? OrderedKey.fromHash(v.targetHash) : new OrderedKey(v);
      const numLeaves = this._r.readUint64();
      data.push(new MetaTuple(ref, key, numLeaves, null));
    }

    return data;
  }

  readIndexedMetaSequence(t: Type): IndexedMetaSequence {
    return new IndexedMetaSequence(this._ds, t, this.readMetaSequence());
  }

  readOrderedMetaSequence(t: Type): OrderedMetaSequence {
    return new OrderedMetaSequence(this._ds, t, this.readMetaSequence());
  }

  readValue(): any {
    const t = this.readType([]);
    switch (t.kind) {
      case Kind.Blob: {
        const isMeta = this._r.readBool();
        if (isMeta) {
          return Blob.fromSequence(this.readIndexedMetaSequence(t));
        }

        return Blob.fromSequence(this.readBlobLeafSequence());
      }
      case Kind.Bool:
        return this._r.readBool();
      case Kind.Number:
        return this._r.readFloat64();
      case Kind.String:
        return this._r.readString();
      case Kind.List: {
        const isMeta = this._r.readBool();
        if (isMeta) {
          return List.fromSequence(this.readIndexedMetaSequence(t));
        }
        return List.fromSequence(this.readListLeafSequence(t));
      }
      case Kind.Map: {
        const isMeta = this._r.readBool();
        if (isMeta) {
          return Map.fromSequence(this.readOrderedMetaSequence(t));
        }
        return Map.fromSequence(this.readMapLeafSequence(t));
      }
      case Kind.Ref:
        return this.readRef(t);
      case Kind.Set: {
        const isMeta = this._r.readBool();
        if (isMeta) {
          return Set.fromSequence(this.readOrderedMetaSequence(t));
        }
        return Set.fromSequence(this.readSetLeafSequence(t));
      }
      case Kind.Struct:
        return this.readStruct(t);
      case Kind.Type:
        return this.readType([]);
      case Kind.Cycle:
      case Kind.Union:
      case Kind.Value:
        throw new Error('A value instance can never have type' + kindToString[t.kind]);
    }

    throw new Error('Unreached');
  }

  readStruct<T: Struct>(type: Type): T {
    const {desc} = type;
    invariant(desc instanceof StructDesc);

    const count = desc.fieldCount;
    const values = new Array(count);
    for (let i = 0; i < count; i++) {
      values[i] = this.readValue();
    }

    return newStructWithValues(type, values);
  }

  readStructType(parentStructTypes: Type[]): Type {
    const name = this._r.readString();
    const count = this._r.readUint32();

    const fields = new Array(count);
    const desc = new StructDesc(name, fields);
    const structType = new Type(desc);
    parentStructTypes.push(structType);

    for (let i = 0; i < count; i++) {
      const name = this._r.readString();
      const type = this.readType(parentStructTypes);
      // Mutate the already created structType since when looking for the cycle we compare
      // by identity.
      fields[i] = {name, type};
    }

    parentStructTypes.pop();
    return structType;
  }
}
