// @flow

import BuzHashBoundaryChecker from './buzhash-boundary-checker.js';
import {default as Ref, sha1Size} from './ref.js';
import type {BoundaryChecker, makeChunkFn} from './sequence-chunker.js';
import type DataStore from './data-store.js';
import type {valueOrPrimitive} from './value.js'; // eslint-disable-line no-unused-vars
import type {Collection} from './collection.js';
import {CompoundDesc, makeCompoundType, numberType, valueType} from './type.js';
import type {Type} from './type.js';
import {IndexedSequence} from './indexed-sequence.js';
import {invariant} from './assert.js';
import {Kind} from './noms-kind.js';
import {OrderedSequence} from './ordered-sequence.js';
import {Sequence} from './sequence.js';

export type MetaSequence = Sequence<MetaTuple>;

export class MetaTuple<K> {
  _sequenceOrRef: Sequence | Ref;
  _value: K;
  _numLeaves: number;

  constructor(sequence: Sequence | Ref, value: K, numLeaves: number) {
    this._sequenceOrRef = sequence;
    this._value = value;
    this._numLeaves = numLeaves;
  }

  get ref(): Ref {
    return this._sequenceOrRef instanceof Ref ? this._sequenceOrRef : this._sequenceOrRef.ref;
  }

  get value(): K {
    return this._value;
  }

  get numLeaves(): number {
    return this._numLeaves;
  }

  get sequence(): ?Sequence {
    return this._sequenceOrRef instanceof Sequence ? this._sequenceOrRef : null;
  }

  getSequence(ds: ?DataStore): Promise<Sequence> {
    if (this._sequenceOrRef instanceof Sequence) {
      return Promise.resolve(this._sequenceOrRef);
    } else {
      const ref = this._sequenceOrRef;
      invariant(ds && ref instanceof Ref);
      return ds.readValue(ref).then((c: Collection) => c.sequence);
    }
  }
}

export class IndexedMetaSequence extends IndexedSequence<MetaTuple<number>> {
  _offsets: Array<number>;

  constructor(ds: ?DataStore, type: Type, items: Array<MetaTuple<number>>) {
    super(ds, type, items);
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
    return mt.getSequence(this.ds);
  }

  // Returns the sequences pointed to by all items[i], s.t. start <= i < end, and returns the
  // concatentation as one long composite sequence
  getCompositeChildSequence(start: number, length: number):
      Promise<IndexedSequence> {
    const childrenP = [];
    for (let i = start; i < start + length; i++) {
      childrenP.push(this.items[i].getSequence(this.ds));
    }

    return Promise.all(childrenP).then(children => {
      const items = [];
      children.forEach(child => items.push(...child.items));
      return children[0].isMeta ? new IndexedMetaSequence(this.ds, this.type, items)
        : new IndexedSequence(this.ds, this.type, items);
    });
  }

  getOffset(idx: number): number {
    return this._offsets[idx] - 1;
  }
}

export class OrderedMetaSequence<K: valueOrPrimitive> extends OrderedSequence<K, MetaTuple<K>> {
  _numLeaves: number;

  constructor(ds: ?DataStore, type: Type, items: Array<MetaTuple<K>>) {
    super(ds, type, items);
    this._numLeaves = items.reduce((l, mt) => l + mt.numLeaves, 0);
  }

  get isMeta(): boolean {
    return true;
  }

  get numLeaves(): number {
    return this._numLeaves;
  }

  getChildSequence(idx: number): Promise<?Sequence> {
    if (!this.isMeta) {
      return Promise.resolve(null);
    }

    const mt = this.items[idx];
    return mt.getSequence(this.ds);
  }

  getKey(idx: number): K {
    return this.items[idx].value;
  }

  equalsAt(idx: number, other: MetaTuple): boolean {
    return this.items[idx].ref.equals(other.ref);
  }
}

export function newMetaSequenceFromData(ds: DataStore, type: Type, tuples: Array<MetaTuple>):
    MetaSequence {
  switch (type.kind) {
    case Kind.Map:
    case Kind.Set:
      return new OrderedMetaSequence(ds, type, tuples);
    case Kind.Blob:
    case Kind.List:
      return new IndexedMetaSequence(ds, type, tuples);
    default:
      throw new Error('Not reached');
  }
}

const indexedSequenceIndexType = numberType;

export function indexTypeForMetaSequence(t: Type): Type {
  switch (t.kind) {
    case Kind.Map:
    case Kind.Set: {
      const desc = t.desc;
      invariant(desc instanceof CompoundDesc);
      const elemType = desc.elemTypes[0];
      if (elemType.ordered) {
        return elemType;
      } else {
        return makeCompoundType(Kind.Ref, valueType);
      }
    }
    case Kind.Blob:
    case Kind.List:
      return indexedSequenceIndexType;
  }

  throw new Error('Not reached');
}

export function newOrderedMetaSequenceChunkFn(t: Type, ds: ?DataStore = null): makeChunkFn {
  return (tuples: Array<MetaTuple>) => {
    const numLeaves = tuples.reduce((l, mt) => l + mt.numLeaves, 0);
    const meta = new OrderedMetaSequence(ds, t, tuples);
    const lastValue = tuples[tuples.length - 1].value;
    return [new MetaTuple(meta, lastValue, numLeaves), meta];
  };
}

const objectWindowSize = 8;
const orderedSequenceWindowSize = 1;
const objectPattern = ((1 << 6) | 0) - 1;

export function newOrderedMetaSequenceBoundaryChecker(): BoundaryChecker<MetaTuple> {
  return new BuzHashBoundaryChecker(orderedSequenceWindowSize, sha1Size, objectPattern,
    (mt: MetaTuple) => mt.ref.digest
  );
}

export function newIndexedMetaSequenceChunkFn(t: Type, ds: ?DataStore = null): makeChunkFn {
  return (tuples: Array<MetaTuple>) => {
    const sum = tuples.reduce((l, mt) => {
      invariant(mt.value === mt.numLeaves);
      return l + mt.value;
    }, 0);
    const meta = new IndexedMetaSequence(ds, t, tuples);
    return [new MetaTuple(meta, sum, sum), meta];
  };
}

export function newIndexedMetaSequenceBoundaryChecker(): BoundaryChecker<MetaTuple> {
  return new BuzHashBoundaryChecker(objectWindowSize, sha1Size, objectPattern,
    (mt: MetaTuple) => mt.ref.digest
  );
}
