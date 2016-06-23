// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import BuzHashBoundaryChecker from './buzhash-boundary-checker.js';
import {compare} from './compare.js';
import {default as Hash, sha1Size} from './hash.js';
import type {BoundaryChecker, makeChunkFn} from './sequence-chunker.js';
import type {ValueReader, ValueWriter} from './value-store.js';
import type Value from './value.js'; // eslint-disable-line no-unused-vars
import {ValueBase} from './value.js';
import type Collection from './collection.js';
import type {Type} from './type.js';
import {makeListType, makeUnionType, blobType, makeSetType, makeMapType} from './type.js';
import {IndexedSequence} from './indexed-sequence.js';
import {invariant, notNull} from './assert.js';
import {OrderedSequence} from './ordered-sequence.js';
import Ref from './ref.js';
import Sequence from './sequence.js';
import {Kind} from './noms-kind.js';
import type {NomsKind} from './noms-kind.js';
import List from './list.js';
import Map from './map.js';
import Set from './set.js';
import Blob from './blob.js';
import type {EqualsFn} from './edit-distance.js';

export type MetaSequence = Sequence<MetaTuple>;

export class MetaTuple<T: Value> {
  ref: Ref;
  key: OrderedKey<T>;
  numLeaves: number;
  child: ?Collection;

  constructor(ref: Ref, key: OrderedKey<T>, numLeaves: number, child: ?Collection) {
    this.ref = ref;
    this.key = key;
    this.numLeaves = numLeaves;
    this.child = child;
  }

  getSequence(vr: ?ValueReader): Promise<Sequence> {
    return this.child ?
        Promise.resolve(this.child.sequence) :
        notNull(vr).readValue(this.ref.targetHash).then((c: Collection) => {
          invariant(c, () => `Could not read sequence ${this.ref.targetHash}`);
          return c.sequence;
        });
  }

  getSequenceSync(): Sequence {
    return notNull(this.child).sequence;
  }
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

  static fromHash(h: Hash): OrderedKey {
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

  compare(other: OrderedKey): number {
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
function getCollectionTypes(tuple: MetaTuple): Type[] {
  return tuple.ref.type.desc.elemTypes[0].desc.elemTypes;
}

export function newListMetaSequence(vr: ?ValueReader, items: Array<MetaTuple>):
    IndexedMetaSequence {
  const t = makeListType(makeUnionType(items.map(tuple => getCollectionTypes(tuple)[0])));
  return new IndexedMetaSequence(vr, t, items);
}

export function newBlobMetaSequence(vr: ?ValueReader, items: Array<MetaTuple>):
    IndexedMetaSequence {
  return new IndexedMetaSequence(vr, blobType, items);
}

export class IndexedMetaSequence extends IndexedSequence<MetaTuple> {
  _offsets: Array<number>;

  constructor(vr: ?ValueReader, t: Type, items: Array<MetaTuple>) {
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

  get chunks(): Array<Ref> {
    return getMetaSequenceChunks(this);
  }

  range(start: number, end: number): Promise<Array<any>> {
    invariant(start >= 0 && end >= 0 && end >= start);

    const childRanges = [];
    for (let i = 0; i < this.items.length && end > start; i++) {
      const cum = this.getOffset(i) + 1;
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

  getChildSequence(idx: number): Promise<?Sequence> {
    if (!this.isMeta) {
      return Promise.resolve(null);
    }

    const mt = this.items[idx];
    return mt.getSequence(this.vr);
  }

  getChildSequenceSync(idx: number): ?Sequence {
    if (!this.isMeta) {
      return null;
    }

    const mt = this.items[idx];
    return mt.getSequenceSync();
  }

  // Returns the sequences pointed to by all items[i], s.t. start <= i < end, and returns the
  // concatentation as one long composite sequence
  getCompositeChildSequence(start: number, length: number):
      Promise<IndexedSequence> {
    const childrenP = [];
    for (let i = start; i < start + length; i++) {
      childrenP.push(this.items[i].getSequence(this.vr));
    }

    return Promise.all(childrenP).then(children => {
      const items = [];
      children.forEach(child => items.push(...child.items));
      return children[0].isMeta ? new IndexedMetaSequence(this.vr, this.type, items)
        : new IndexedSequence(this.vr, this.type, items);
    });
  }

  getOffset(idx: number): number {
    return this._offsets[idx] - 1;
  }

  getCompareFn(other: IndexedSequence): EqualsFn {
    return (idx: number, otherIdx: number) =>
      this.items[idx].ref.targetHash.equals(other.items[otherIdx].ref.targetHash);
  }
}

export function newMapMetaSequence<K: Value>(vr: ?ValueReader,
    tuples: Array<MetaTuple>): OrderedMetaSequence<K> {
  const kt = makeUnionType(tuples.map(mt => getCollectionTypes(mt)[0]));
  const vt = makeUnionType(tuples.map(mt => getCollectionTypes(mt)[1]));
  const t = makeMapType(kt, vt);
  return new OrderedMetaSequence(vr, t, tuples);
}

export function newSetMetaSequence<K: Value>(vr: ?ValueReader,
    tuples: Array<MetaTuple>): OrderedMetaSequence<K> {
  const t = makeSetType(makeUnionType(tuples.map(mt => getCollectionTypes(mt)[0])));
  return new OrderedMetaSequence(vr, t, tuples);
}

export class OrderedMetaSequence<K: Value> extends OrderedSequence<K, MetaTuple> {
  _numLeaves: number;

  constructor(vr: ?ValueReader, t: Type, items: Array<MetaTuple>) {
    super(vr, t, items);
    this._numLeaves = items.reduce((l, mt) => l + mt.numLeaves, 0);
  }

  get isMeta(): boolean {
    return true;
  }

  get numLeaves(): number {
    return this._numLeaves;
  }

  get chunks(): Array<Ref> {
    return getMetaSequenceChunks(this);
  }

  getChildSequence(idx: number): Promise<?Sequence> {
    if (!this.isMeta) {
      return Promise.resolve(null);
    }

    const mt = this.items[idx];
    return mt.getSequence(this.vr);
  }

  getChildSequenceSync(idx: number): ?Sequence {
    if (!this.isMeta) {
      return null;
    }

    const mt = this.items[idx];
    return mt.getSequenceSync();
  }

  getKey(idx: number): OrderedKey {
    return this.items[idx].key;
  }

  getCompareFn(other: OrderedSequence): EqualsFn {
    return (idx: number, otherIdx: number) =>
      this.items[idx].ref.targetHash.equals(other.items[otherIdx].ref.targetHash);
  }
}

export function newOrderedMetaSequenceChunkFn(kind: NomsKind, vr: ?ValueReader): makeChunkFn {
  return (tuples: Array<MetaTuple>) => {
    const numLeaves = tuples.reduce((l, mt) => l + mt.numLeaves, 0);
    const last = tuples[tuples.length - 1];
    let seq: OrderedMetaSequence;
    let col: Collection;
    if (kind === Kind.Map) {
      seq = newMapMetaSequence(vr, tuples);
      col = Map.fromSequence(seq);
    } else {
      invariant(kind === Kind.Set);
      seq = newSetMetaSequence(vr, tuples);
      col = Set.fromSequence(seq);
    }
    return [new MetaTuple(new Ref(col), last.key, numLeaves, col), seq];
  };
}

const objectWindowSize = 8;
const orderedSequenceWindowSize = 1;
const objectPattern = ((1 << 6) | 0) - 1;

export function newOrderedMetaSequenceBoundaryChecker(): BoundaryChecker<MetaTuple> {
  return new BuzHashBoundaryChecker(orderedSequenceWindowSize, sha1Size, objectPattern,
    (mt: MetaTuple) => mt.ref.targetHash.digest
  );
}

export function newIndexedMetaSequenceChunkFn(kind: NomsKind, vr: ?ValueReader,
                                              vw: ?ValueWriter): makeChunkFn {
  return (tuples: Array<MetaTuple>) => {
    const sum = tuples.reduce((l, mt) => {
      const nv = mt.key.numberValue();
      invariant(nv === mt.numLeaves);
      return l + nv;
    }, 0);
    let seq: IndexedMetaSequence;
    let col: Collection;
    if (kind === Kind.List) {
      seq = newListMetaSequence(vr, tuples);
      col = List.fromSequence(seq);
    } else {
      invariant(kind === Kind.Blob);
      seq = newBlobMetaSequence(vr, tuples);
      col = Blob.fromSequence(seq);
    }
    const key = new OrderedKey(sum);
    let mt;
    if (vw) {
      mt = new MetaTuple(vw.writeValue(col), key, sum, null);
    } else {
      mt = new MetaTuple(new Ref(col), key, sum, col);
    }
    return [mt, seq];
  };
}

export function newIndexedMetaSequenceBoundaryChecker(): BoundaryChecker<MetaTuple> {
  return new BuzHashBoundaryChecker(objectWindowSize, sha1Size, objectPattern,
    (mt: MetaTuple) => mt.ref.targetHash.digest
  );
}

function getMetaSequenceChunks(ms: MetaSequence): Array<Ref> {
  return ms.items.map(mt => mt.ref);
}
