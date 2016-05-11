// @flow

import BuzHashBoundaryChecker from './buzhash-boundary-checker.js';
import {sha1Size} from './ref.js';
import type {BoundaryChecker, makeChunkFn} from './sequence-chunker.js';
import type {ValueReader} from './value-store.js';
import type {valueOrPrimitive} from './value.js'; // eslint-disable-line no-unused-vars
import type {Collection} from './collection.js';
import type {Type} from './type.js';
import {IndexedSequence} from './indexed-sequence.js';
import {invariant, notNull} from './assert.js';
import {OrderedSequence} from './ordered-sequence.js';
import RefValue from './ref-value.js';
import {Sequence} from './sequence.js';
export type MetaSequence = Sequence<MetaTuple>;
import {Kind} from './noms-kind.js';
import {NomsList} from './list.js';
import {NomsMap} from './map.js';
import {NomsSet} from './set.js';
import {NomsBlob} from './blob.js';

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

export class IndexedMetaSequence extends IndexedSequence<MetaTuple<number>> {
  _offsets: Array<number>;

  constructor(vr: ?ValueReader, type: Type, items: Array<MetaTuple<number>>) {
    super(vr, type, items);
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

export class OrderedMetaSequence<K: valueOrPrimitive> extends OrderedSequence<K, MetaTuple<K>> {
  _numLeaves: number;

  constructor(vr: ?ValueReader, type: Type, items: Array<MetaTuple<K>>) {
    super(vr, type, items);
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
    return this.items[idx].ref.equals(other.ref);
  }
}

export function newOrderedMetaSequenceChunkFn(t: Type, vr: ?ValueReader = null): makeChunkFn {
  return (tuples: Array<MetaTuple>) => {
    const numLeaves = tuples.reduce((l, mt) => l + mt.numLeaves, 0);
    const metaSeq = new OrderedMetaSequence(vr, t, tuples);
    const last = tuples[tuples.length - 1];
    let col: Collection;
    if (t.kind === Kind.Map) {
      col = new NomsMap(metaSeq);
    } else {
      invariant(t.kind === Kind.Set);
      col = new NomsSet(metaSeq);
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

export function newIndexedMetaSequenceChunkFn(t: Type, vr: ?ValueReader = null): makeChunkFn {
  return (tuples: Array<MetaTuple>) => {
    const sum = tuples.reduce((l, mt) => {
      invariant(mt.value === mt.numLeaves);
      return l + mt.value;
    }, 0);
    const metaSeq = new IndexedMetaSequence(vr, t, tuples);
    let col: Collection;
    if (t.kind === Kind.List) {
      col = new NomsList(metaSeq);
    } else {
      invariant(t.kind === Kind.Blob);
      col = new NomsBlob(metaSeq);
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
