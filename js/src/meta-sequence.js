// @flow

import BuzHashBoundaryChecker from './buzhash-boundary-checker.js';
import {sha1Size} from './ref.js';
import type {BoundaryChecker, makeChunkFn} from './sequence-chunker.js';
import type {ValueReader} from './value-store.js';
import type Value from './value.js'; // eslint-disable-line no-unused-vars
import type {Collection} from './collection.js';
import type {Type} from './type.js';
import {makeListType, makeUnionType, blobType, makeSetType, makeMapType} from './type.js';
import {IndexedSequence} from './indexed-sequence.js';
import {invariant, notNull} from './assert.js';
import {OrderedSequence} from './ordered-sequence.js';
import RefValue from './ref-value.js';
import Sequence from './sequence.js';
import {Kind} from './noms-kind.js';
import type {NomsKind} from './noms-kind.js';
import {newListFromSequence} from './list.js';
import {newMapFromSequence} from './map.js';
import {newSetFromSequence} from './set.js';
import {newBlobFromSequence} from './blob.js';
import {equals} from './compare.js';

export type MetaSequence = Sequence<MetaTuple>;

export class MetaTuple<K> {
  ref: RefValue;
  value: K;
  numLeaves: number;
  child: ?Collection;

  constructor(ref: RefValue, value: K, numLeaves: number, child: ?Collection = null) {
    this.ref = ref;
    this.child = child;
    this.value = value;
    this.numLeaves = numLeaves;
  }

  getSequence(vr: ?ValueReader): Promise<Sequence> {
    return this.child ?
        Promise.resolve(this.child.sequence) :
        notNull(vr).readValue(this.ref.targetRef).then((c: Collection) => {
          invariant(c, () => `Could not read sequence ${this.ref.targetRef}`);
          return c.sequence;
        });
  }
}

// The elemTypes of the collection inside the Ref<Collection<?, ?>>
function getCollectionTypes<K>(tuple: MetaTuple<K>): Type[] {
  return tuple.ref.type.desc.elemTypes[0].desc.elemTypes;
}

export function newListMetaSequence(vr: ?ValueReader, items: Array<MetaTuple<number>>):
    IndexedMetaSequence {
  const t = makeListType(makeUnionType(items.map(tuple => getCollectionTypes(tuple)[0])));
  return new IndexedMetaSequence(vr, t, items);
}

export function newBlobMetaSequence(vr: ?ValueReader, items: Array<MetaTuple<number>>):
    IndexedMetaSequence {
  return new IndexedMetaSequence(vr, blobType, items);
}

export class IndexedMetaSequence extends IndexedSequence<MetaTuple<number>> {
  _offsets: Array<number>;

  constructor(vr: ?ValueReader, t: Type, items: Array<MetaTuple<number>>) {
    super(vr, t, items);
    let cum = 0;
    this._offsets = this.items.map(i => {
      cum += i.value;
      return cum;
    });
  }

  get isMeta(): boolean {
    return true;
  }

  get numLeaves(): number {
    return this._offsets[this._offsets.length - 1];
  }

  get chunks(): Array<RefValue> {
    return getMetaSequenceChunks(this);
  }

  range(start: number, end: number): Promise<Array<any>> {
    invariant(start >= 0 && end >= 0 && end >= start);

    const childRanges = [];
    for (let i = 0; i < this.items.length && end > start; i++) {
      const cum = this.getOffset(i) + 1;
      const seqLength = this.items[i].value;
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
}

export function newMapMetaSequence<K: Value>(vr: ?ValueReader,
    tuples: Array<MetaTuple<K>>): OrderedMetaSequence<K> {
  const kt = makeUnionType(tuples.map(mt => getCollectionTypes(mt)[0]));
  const vt = makeUnionType(tuples.map(mt => getCollectionTypes(mt)[1]));
  const t = makeMapType(kt, vt);
  return new OrderedMetaSequence(vr, t, tuples);
}

export function newSetMetaSequence<K: Value>(vr: ?ValueReader,
    tuples: Array<MetaTuple<K>>): OrderedMetaSequence<K> {
  const t = makeSetType(makeUnionType(tuples.map(mt => getCollectionTypes(mt)[0])));
  return new OrderedMetaSequence(vr, t, tuples);
}

export class OrderedMetaSequence<K: Value> extends OrderedSequence<K, MetaTuple<K>> {
  _numLeaves: number;

  constructor(vr: ?ValueReader, t: Type, items: Array<MetaTuple<K>>) {
    super(vr, t, items);
    this._numLeaves = items.reduce((l, mt) => l + mt.numLeaves, 0);
  }

  get isMeta(): boolean {
    return true;
  }

  get numLeaves(): number {
    return this._numLeaves;
  }

  get chunks(): Array<RefValue> {
    return getMetaSequenceChunks(this);
  }

  getChildSequence(idx: number): Promise<?Sequence> {
    if (!this.isMeta) {
      return Promise.resolve(null);
    }

    const mt = this.items[idx];
    return mt.getSequence(this.vr);
  }

  getKey(idx: number): K {
    return this.items[idx].value;
  }

  equalsAt(idx: number, other: MetaTuple): boolean {
    return equals(this.items[idx].ref, other.ref);
  }
}

export function newOrderedMetaSequenceChunkFn(kind: NomsKind, vr: ?ValueReader): makeChunkFn {
  return (tuples: Array<MetaTuple>) => {
    const numLeaves = tuples.reduce((l, mt) => l + mt.numLeaves, 0);
    const last = tuples[tuples.length - 1];
    let col: Collection;
    if (kind === Kind.Map) {
      col = newMapFromSequence(newMapMetaSequence(vr, tuples));
    } else {
      invariant(kind === Kind.Set);
      col = newSetFromSequence(newSetMetaSequence(vr, tuples));
    }
    return [new MetaTuple(new RefValue(col), last.value, numLeaves, col), col];
  };
}

const objectWindowSize = 8;
const orderedSequenceWindowSize = 1;
const objectPattern = ((1 << 6) | 0) - 1;

export function newOrderedMetaSequenceBoundaryChecker(): BoundaryChecker<MetaTuple> {
  return new BuzHashBoundaryChecker(orderedSequenceWindowSize, sha1Size, objectPattern,
    (mt: MetaTuple) => mt.ref.targetRef.digest
  );
}

export function newIndexedMetaSequenceChunkFn(kind: NomsKind, vr: ?ValueReader): makeChunkFn {
  return (tuples: Array<MetaTuple>) => {
    const sum = tuples.reduce((l, mt) => {
      invariant(mt.value === mt.numLeaves);
      return l + mt.value;
    }, 0);
    let col: Collection;
    if (kind === Kind.List) {
      col = newListFromSequence(newListMetaSequence(vr, tuples));
    } else {
      invariant(kind === Kind.Blob);
      col = newBlobFromSequence(newBlobMetaSequence(vr, tuples));
    }
    return [new MetaTuple(new RefValue(col), sum, sum, col), col];
  };
}

export function newIndexedMetaSequenceBoundaryChecker(): BoundaryChecker<MetaTuple> {
  return new BuzHashBoundaryChecker(objectWindowSize, sha1Size, objectPattern,
    (mt: MetaTuple) => mt.ref.targetRef.digest
  );
}

function getMetaSequenceChunks(ms: MetaSequence): Array<RefValue> {
  return ms.items.map(mt => mt.ref);
}
