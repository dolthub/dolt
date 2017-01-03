// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import type {makeChunkFn} from './sequence-chunker.js';
import type {ValueReader} from './value-store.js';
import type Value from './value.js'; // eslint-disable-line no-unused-vars
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
import {invariant, notNull} from './assert.js';
import Ref, {constructRef} from './ref.js';
import Sequence, {OrderedKey} from './sequence.js';
import {Kind} from './noms-kind.js';
import type {NomsKind} from './noms-kind.js';
import List from './list.js';
import Map from './map.js';
import Set from './set.js';
import Blob from './blob.js';
import type {EqualsFn} from './edit-distance.js';
import {hashValueBytes} from './rolling-value-hasher.js';
import RollingValueHasher from './rolling-value-hasher.js';
import {ListLeafSequence} from './list.js';

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

// The elemTypes of the collection inside the Ref<Collection<?, ?>>
function getCollectionTypes(tuple: MetaTuple<any>): Type<any>[] {
  return tuple.ref.type.desc.elemTypes[0].desc.elemTypes;
}

export function newListMetaSequence(vr: ?ValueReader, items: Array<MetaTuple<any>>)
    : MetaSequence {
  const t = makeListType(makeUnionType(items.map(tuple => getCollectionTypes(tuple)[0])));
  return new MetaSequence(vr, t, items);
}

export function newBlobMetaSequence(vr: ?ValueReader, items: Array<MetaTuple<any>>)
    : MetaSequence {
  return new MetaSequence(vr, blobType, items);
}

class EmptySequence extends Sequence {
  constructor() {
    super(null, valueType, []);
  }
}

// Returns the sequences pointed to by all items[i], s.t. start <= i < end, and returns the
// concatentation as one long composite sequence
export function getCompositeChildSequence(sequence: Sequence<any>, start: number,
    length: number): Promise<Sequence<any>> {
  if (length === 0) {
    return Promise.resolve(new EmptySequence());
  }

  const childrenP = [];
  for (let i = start; i < start + length; i++) {
    childrenP.push(sequence.items[i].getChildSequence(sequence.vr));
  }

  return Promise.all(childrenP).then(children => {
    const items = [];
    children.forEach(child => items.push(...child.items));
    if (!children[0].isMeta) {
      // Any because our type params are all screwy and FlowIssue didn't suppress the error.
      return new ListLeafSequence(sequence.vr, sequence.type, (items: any));
    }

    return new MetaSequence(sequence.vr, sequence.type, items);
  });
}

export class MetaSequence extends Sequence<MetaTuple<any>> {

  constructor(vr: ?ValueReader, t: Type<any>, items: Array<MetaTuple<any>>) {
    super(vr, t, items);
  }

  get isMeta(): boolean {
    return true;
  }

  get numLeaves(): number {
    return this.cumulativeNumberOfLeaves(this.items.length - 1);
  }

  get chunks(): Array<Ref<any>> {
    return this.items.map(mt => mt.ref);
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

  cumulativeNumberOfLeaves(idx: number): number {
    let cum = 0;
    for (let i = 0; i <= idx; i++) {
      cum += this.items[i].numLeaves;
    }

    return cum;
  }

  getKey(idx: number): OrderedKey<any> {
    return this.items[idx].key;
  }

  getCompareFn(other: Sequence<any>): EqualsFn {
    return (idx: number, otherIdx: number) =>
      this.items[idx].ref.targetHash.equals(other.items[otherIdx].ref.targetHash);
  }
}

export function newMapMetaSequence(vr: ?ValueReader, tuples: Array<MetaTuple<any>>): MetaSequence {
  const kt = makeUnionType(tuples.map(mt => getCollectionTypes(mt)[0]));
  const vt = makeUnionType(tuples.map(mt => getCollectionTypes(mt)[1]));
  const t = makeMapType(kt, vt);
  return new MetaSequence(vr, t, tuples);
}

export function newSetMetaSequence(vr: ?ValueReader, tuples: Array<MetaTuple<any>>): MetaSequence {
  const t = makeSetType(makeUnionType(tuples.map(mt => getCollectionTypes(mt)[0])));
  return new MetaSequence(vr, t, tuples);
}

export function newOrderedMetaSequenceChunkFn(kind: NomsKind, vr: ?ValueReader)
    : makeChunkFn<any, any> {
  return (tuples: Array<MetaTuple<any>>) => {
    const numLeaves = tuples.reduce((l, mt) => l + mt.numLeaves, 0);
    const last = tuples[tuples.length - 1];
    let seq: MetaSequence;
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
    let seq: MetaSequence;
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

