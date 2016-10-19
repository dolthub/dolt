// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {compare} from './compare.js';
import Hash from './hash.js';
import type {makeChunkFn} from './sequence-chunker.js';
import type {ValueReader} from './value-store.js';
import type Value from './value.js'; // eslint-disable-line no-unused-vars
import {ValueBase} from './value.js';
import Collection from './collection.js';
import type {Type} from './type.js';
import {
  boolType,
  blobType,
  makeMapType,
  makeListType,
  makeRefType,
  makeSetType,
  makeUnionType,
  valueType,
} from './type.js';
import {IndexedSequence} from './indexed-sequence.js';
import {invariant, notNull} from './assert.js';
import {OrderedSequence} from './ordered-sequence.js';
import Ref, {constructRef} from './ref.js';
import Sequence from './sequence.js';
import {Kind} from './noms-kind.js';
import type {NomsKind} from './noms-kind.js';
import List, {ListLeafSequence} from './list.js';
import Map from './map.js';
import Set from './set.js';
import Blob from './blob.js';
import type {EqualsFn} from './edit-distance.js';
import {hashValueBytes} from './rolling-value-hasher.js';
import RollingValueHasher from './rolling-value-hasher.js';

export type MetaSequence<T: Value> = Sequence<MetaTuple<T>>;

export class MetaTuple<T: Value> {
  ref: Ref<any>;
  key: OrderedKey<T>;
  numLeaves: number;
  child: ?Collection<any>;

  constructor(ref: Ref<any>, key: OrderedKey<T>, numLeaves: number, child: ?Collection<any>) {
    this.ref = ref;
    this.key = key;
    this.numLeaves = numLeaves;
    this.child = child;
  }

  getChildSequence(vr: ?ValueReader): Promise<Sequence<any>> {
    return this.child ?
        Promise.resolve(this.child.sequence) :
        notNull(vr).readValue(this.ref.targetHash).then((c: Collection<any>) => {
          invariant(c, () => `Could not read sequence ${this.ref.targetHash.toString()}`);
          return c.sequence;
        });
  }

  getChildSequenceSync(): Sequence<any> {
    return notNull(this.child).sequence;
  }
}

export function metaHashValueBytes(tuple: MetaTuple<any>, rv: RollingValueHasher) {
  let val = tuple.key.v;
  if (!tuple.key.isOrderedByValue) {
    // See https://github.com/attic-labs/noms/issues/1688#issuecomment-227528987
    val = constructRef(makeRefType(boolType), notNull(tuple.key.h), 0);
  } else {
    val = notNull(val);
  }

  hashValueBytes(tuple.ref, rv);
  hashValueBytes(val, rv);
}

export class OrderedKey<T: Value> {
  isOrderedByValue: boolean;
  v: ?T;
  h: ?Hash;

  constructor(v: T) {
    this.v = v;
    if (v instanceof ValueBase) {
      this.isOrderedByValue = false;
      this.h = v.hash;
    } else {
      this.isOrderedByValue = true;
      this.h = null;
    }
  }

  static fromHash(h: Hash): OrderedKey<any> {
    const k = Object.create(this.prototype);
    k.isOrderedByValue = false;
    k.v = null;
    k.h = h;
    return k;
  }

  value(): T {
    return notNull(this.v);
  }

  numberValue(): number {
    invariant(typeof this.v === 'number');
    return this.v;
  }

  compare(other: OrderedKey<any>): number {
    if (this.isOrderedByValue && other.isOrderedByValue) {
      return compare(notNull(this.v), notNull(other.v));
    }
    if (this.isOrderedByValue) {
      return -1;
    }
    if (other.isOrderedByValue) {
      return 1;
    }
    return notNull(this.h).compare(notNull(other.h));
  }
}

// The elemTypes of the collection inside the Ref<Collection<?, ?>>
function getCollectionTypes(tuple: MetaTuple<any>): Type<any>[] {
  return tuple.ref.type.desc.elemTypes[0].desc.elemTypes;
}

export function newListMetaSequence(vr: ?ValueReader, items: Array<MetaTuple<any>>)
    : IndexedMetaSequence {
  const t = makeListType(makeUnionType(items.map(tuple => getCollectionTypes(tuple)[0])));
  return new IndexedMetaSequence(vr, t, items);
}

export function newBlobMetaSequence(vr: ?ValueReader, items: Array<MetaTuple<any>>)
    : IndexedMetaSequence {
  return new IndexedMetaSequence(vr, blobType, items);
}

export class IndexedMetaSequence extends IndexedSequence<MetaTuple<any>> {
  _offsets: Array<number>;

  constructor(vr: ?ValueReader, t: Type<any>, items: Array<MetaTuple<any>>) {
    super(vr, t, items);
    let cum = 0;
    this._offsets = this.items.map(i => {
      cum += i.key.numberValue();
      return cum;
    });
  }

  get isMeta(): boolean {
    return true;
  }

  get numLeaves(): number {
    return this._offsets[this._offsets.length - 1];
  }

  get chunks(): Array<Ref<any>> {
    return getMetaSequenceChunks(this);
  }

  range(start: number, end: number): Promise<Array<any>> {
    invariant(start >= 0 && end >= 0 && end >= start);

    const childRanges = [];
    for (let i = 0; i < this.items.length && end > start; i++) {
      const cum = this.cumulativeNumberOfLeaves(i);
      const seqLength = this.items[i].key.numberValue();
      if (start < cum) {
        const seqStart = cum - seqLength;
        const childStart = start - seqStart;
        const childEnd = Math.min(seqLength, end - seqStart);
        childRanges.push(this.getChildSequence(i).then(child => {
          invariant(child instanceof IndexedSequence);
          return child.range(childStart, childEnd);
        }));
        start += childEnd - childStart;
      }
    }

    return Promise.all(childRanges).then(ranges => {
      const range = [];
      ranges.forEach(r => range.push(...r));
      return range;
    });
  }

  getChildSequence(idx: number): Promise<?Sequence<any>> {
    if (!this.isMeta) {
      return Promise.resolve(null);
    }

    const mt = this.items[idx];
    return mt.getChildSequence(this.vr);
  }

  getChildSequenceSync(idx: number): ?Sequence<any> {
    if (!this.isMeta) {
      return null;
    }

    const mt = this.items[idx];
    return mt.getChildSequenceSync();
  }

  // Returns the sequences pointed to by all items[i], s.t. start <= i < end, and returns the
  // concatentation as one long composite sequence
  getCompositeChildSequence(start: number, length: number): Promise<IndexedSequence<any>> {
    if (length === 0) {
      return Promise.resolve(new EmptySequence());
    }

    const childrenP = [];
    for (let i = start; i < start + length; i++) {
      childrenP.push(this.items[i].getChildSequence(this.vr));
    }

    return Promise.all(childrenP).then(children => {
      const items = [];
      children.forEach(child => items.push(...child.items));
      if (!children[0].isMeta) {
        // Any because our type params are all screwy and FlowIssue didn't suppress the error.
        return new ListLeafSequence(this.vr, this.type, (items: any));
      }

      return new IndexedMetaSequence(this.vr, this.type, items);
    });
  }

  cumulativeNumberOfLeaves(idx: number): number {
    return this._offsets[idx];
  }

  getCompareFn(other: IndexedSequence<any>): EqualsFn {
    return (idx: number, otherIdx: number) =>
      this.items[idx].ref.targetHash.equals(other.items[otherIdx].ref.targetHash);
  }
}

export function newMapMetaSequence<K: Value>(vr: ?ValueReader,
    tuples: Array<MetaTuple<any>>): OrderedMetaSequence<K> {
  const kt = makeUnionType(tuples.map(mt => getCollectionTypes(mt)[0]));
  const vt = makeUnionType(tuples.map(mt => getCollectionTypes(mt)[1]));
  const t = makeMapType(kt, vt);
  return new OrderedMetaSequence(vr, t, tuples);
}

export function newSetMetaSequence<K: Value>(vr: ?ValueReader,
    tuples: Array<MetaTuple<any>>): OrderedMetaSequence<K> {
  const t = makeSetType(makeUnionType(tuples.map(mt => getCollectionTypes(mt)[0])));
  return new OrderedMetaSequence(vr, t, tuples);
}

export class OrderedMetaSequence<K: Value> extends OrderedSequence<K, MetaTuple<any>> {
  _numLeaves: number;

  constructor(vr: ?ValueReader, t: Type<any>, items: Array<MetaTuple<any>>) {
    super(vr, t, items);
    this._numLeaves = items.reduce((l, mt) => l + mt.numLeaves, 0);
  }

  get isMeta(): boolean {
    return true;
  }

  get numLeaves(): number {
    return this._numLeaves;
  }

  get chunks(): Array<Ref<any>> {
    return getMetaSequenceChunks(this);
  }

  getChildSequence(idx: number): Promise<?Sequence<any>> {
    if (!this.isMeta) {
      return Promise.resolve(null);
    }

    const mt = this.items[idx];
    return mt.getChildSequence(this.vr);
  }

  getChildSequenceSync(idx: number): ?Sequence<any> {
    if (!this.isMeta) {
      return null;
    }

    const mt = this.items[idx];
    return mt.getChildSequenceSync();
  }

  getKey(idx: number): OrderedKey<any> {
    return this.items[idx].key;
  }

  getCompareFn(other: OrderedSequence<any, any>): EqualsFn {
    return (idx: number, otherIdx: number) =>
      this.items[idx].ref.targetHash.equals(other.items[otherIdx].ref.targetHash);
  }
}

export function newOrderedMetaSequenceChunkFn(kind: NomsKind, vr: ?ValueReader)
    : makeChunkFn<any, any> {
  return (tuples: Array<MetaTuple<any>>) => {
    const numLeaves = tuples.reduce((l, mt) => l + mt.numLeaves, 0);
    const last = tuples[tuples.length - 1];
    let seq: OrderedMetaSequence<any>;
    let col: Collection<any>;
    if (kind === Kind.Map) {
      seq = newMapMetaSequence(vr, tuples);
      col = Map.fromSequence(seq);
    } else {
      invariant(kind === Kind.Set);
      seq = newSetMetaSequence(vr, tuples);
      col = Set.fromSequence(seq);
    }
    return [col, last.key, numLeaves];
  };
}

export function newIndexedMetaSequenceChunkFn(kind: NomsKind, vr: ?ValueReader)
    : makeChunkFn<any, any> {
  return (tuples: Array<MetaTuple<any>>) => {
    const sum = tuples.reduce((l, mt) => {
      const nv = mt.key.numberValue();
      invariant(nv === mt.numLeaves);
      return l + nv;
    }, 0);
    let seq: IndexedMetaSequence;
    let col: Collection<any>;
    if (kind === Kind.List) {
      seq = newListMetaSequence(vr, tuples);
      col = List.fromSequence(seq);
    } else {
      invariant(kind === Kind.Blob);
      seq = newBlobMetaSequence(vr, tuples);
      col = Blob.fromSequence(seq);
    }
    const key = new OrderedKey(sum);
    return [col, key, sum];
  };
}

function getMetaSequenceChunks(ms: MetaSequence<any>): Array<Ref<any>> {
  return ms.items.map(mt => mt.ref);
}

class EmptySequence extends IndexedSequence {
  constructor() {
    super(null, valueType, []);
  }
}
