// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import Blob, {BlobLeafSequence} from './blob.js';
import Ref, {constructRef} from './ref.js';
import {newStructWithType} from './struct.js';
import type Struct from './struct.js';
import type {NomsKind} from './noms-kind.js';
import {
  getPrimitiveType,
  StructDesc,
  Type,
} from './type.js';
import {OrderedKey, MetaTuple} from './meta-sequence.js';
import {invariant, notNull} from './assert.js';
import {isPrimitiveKind, kindToString, Kind} from './noms-kind.js';
import List, {ListLeafSequence} from './list.js';
import Map, {MapLeafSequence} from './map.js';
import Set, {SetLeafSequence} from './set.js';
import {IndexedMetaSequence, OrderedMetaSequence} from './meta-sequence.js';
import type Value from './value.js';
import type {ValueReader} from './value-store.js';
import type {NomsReader} from './codec.js';
import type TypeCache from './type-cache.js';

export default class ValueDecoder {
  _r: NomsReader;
  _ds: ValueReader;
  _tc: TypeCache;

  constructor(r: NomsReader, vr: ValueReader, tc: TypeCache) {
    this._r = r;
    this._ds = vr;
    this._tc = tc;
  }

  readKind(): NomsKind {
    return this._r.readUint8();
  }

  readRef(t: Type<any>): Ref<any> {
    const hash = this._r.readHash();
    const height = this._r.readUint64();
    return constructRef(t, hash, height);
  }

  readType(): Type<any> {
    const k = this.readKind();
    switch (k) {
      case Kind.List:
        return this._tc.getCompoundType(k, this.readType());
      case Kind.Map:
        return this._tc.getCompoundType(k, this.readType(), this.readType());
      case Kind.Set:
        return this._tc.getCompoundType(k, this.readType());
      case Kind.Ref:
        return this._tc.getCompoundType(k, this.readType());
      case Kind.Struct:
        return this.readStructType();
      case Kind.Union: {
        const len = this._r.readUint32();
        const types: Type<any>[] = new Array(len);
        for (let i = 0; i < len; i++) {
          types[i] = this.readType();
        }
        return this._tc.getCompoundType(k, ...types);
      }
      case Kind.Cycle: {
        return this._tc.getCycleType(this._r.readUint32());
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

  readListLeafSequence(t: Type<any>): ListLeafSequence<any> {
    const data = this.readValueSequence();
    return new ListLeafSequence(this._ds, t, data);
  }

  readSetLeafSequence(t: Type<any>): SetLeafSequence<any> {
    const data = this.readValueSequence();
    return new SetLeafSequence(this._ds, t, data);
  }

  readMapLeafSequence(t: Type<any>): MapLeafSequence<any, any> {
    const count = this._r.readUint32();
    const data = [];
    for (let i = 0; i < count; i++) {
      const k = this.readValue();
      const v = this.readValue();
      data.push([k, v]);
    }

    return new MapLeafSequence(this._ds, t, data);
  }

  readMetaSequence(): Array<MetaTuple<any>> {
    const count = this._r.readUint32();

    const data: Array<MetaTuple<any>> = [];
    for (let i = 0; i < count; i++) {
      const ref = this.readValue();
      const v = this.readValue();
      const key = v instanceof Ref ? OrderedKey.fromHash(v.targetHash) : new OrderedKey(v);
      const numLeaves = this._r.readUint64();
      data.push(new MetaTuple(ref, key, numLeaves, null));
    }

    return data;
  }

  readIndexedMetaSequence(t: Type<any>): IndexedMetaSequence {
    return new IndexedMetaSequence(this._ds, t, this.readMetaSequence());
  }

  readOrderedMetaSequence(t: Type<any>): OrderedMetaSequence<any> {
    return new OrderedMetaSequence(this._ds, t, this.readMetaSequence());
  }

  readValue(): any {
    const t = this.readType();
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
        return this._r.readNumber();
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
        return this.readType();
      case Kind.Cycle:
      case Kind.Union:
      case Kind.Value:
        throw new Error('A value instance can never have type' + kindToString[t.kind]);
    }

    throw new Error('Unreached');
  }

  readStruct<T: Struct>(type: Type<any>): T {
    const {desc} = type;
    invariant(desc instanceof StructDesc);

    const count = desc.fieldCount;
    const values = new Array(count);
    for (let i = 0; i < count; i++) {
      values[i] = this.readValue();
    }

    return newStructWithType(type, values);
  }

  readCachedStructType(): ?Type<StructDesc> {
    let trie = notNull(this._tc.trieRoots.get(Kind.Struct)).traverse(this._r.readIdent(this._tc));
    const count = this._r.readUint32();
    for (let i = 0; i < count; i++) {
      trie = trie.traverse(this._r.readIdent(this._tc));
      trie = trie.traverse(this.readType().id);
    }

    return trie.t;
  }

  readStructType(): Type<StructDesc> {
    const pos = this._r.pos();
    const t = this.readCachedStructType();
    if (t) {
      return t;
    }
    this._r.seek(pos);

    const name = this._r.readString();
    const count = this._r.readUint32();

    const fieldNames = new Array(count);
    const fieldTypes = new Array(count);
    for (let i = 0; i < count; i++) {
      fieldNames[i] = this._r.readString();
      fieldTypes[i] = this.readType();
    }
    return this._tc.makeStructTypeQuickly(name, fieldNames, fieldTypes);
  }
}
