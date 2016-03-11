// @flow

import BuzHashBoundaryChecker from './buzhash_boundary_checker.js';
import {default as Ref, sha1Size} from './ref.js';
import type {BoundaryChecker, makeChunkFn} from './sequence_chunker.js';
import type {ChunkStore} from './chunk_store.js';
import type {valueOrPrimitive} from './value.js'; // eslint-disable-line no-unused-vars
import {Collection} from './collection.js';
import {CompoundDesc, makeCompoundType, makePrimitiveType, Type} from './type.js';
import {IndexedSequence} from './indexed_sequence.js';
import {invariant} from './assert.js';
import {Kind} from './noms_kind.js';
import {OrderedSequence} from './ordered_sequence.js';
import {readValue} from './read_value.js';
import {Sequence} from './sequence.js';

export type MetaSequence = Sequence<MetaTuple>;

export class MetaTuple<K> {
  _sequenceOrRef: Sequence | Ref;
  value: K;

  constructor(sequence: Sequence | Ref, value: K) {
    this._sequenceOrRef = sequence;
    this.value = value;
  }

  get ref(): Ref {
    return this._sequenceOrRef instanceof Ref ? this._sequenceOrRef : this._sequenceOrRef.ref;
  }

  get sequence(): ?Sequence {
    return this._sequenceOrRef instanceof Sequence ? this._sequenceOrRef : null;
  }

  getSequence(cs: ?ChunkStore): Promise<Sequence> {
    if (this._sequenceOrRef instanceof Sequence) {
      return Promise.resolve(this._sequenceOrRef);
    } else {
      const ref = this._sequenceOrRef;
      invariant(cs && ref instanceof Ref);
      return readValue(ref, cs).then((c: Collection) => c.sequence);
    }
  }
}

export class IndexedMetaSequence extends IndexedSequence<MetaTuple<number>> {
  offsets: Array<number>;

  constructor(cs: ?ChunkStore, type: Type, items: Array<MetaTuple<number>>) {
    super(cs, type, items);
    this.isMeta = true;
    this.offsets = [];
    let cum = 0;
    for (let i = 0; i < items.length; i++) {
      const length = items[i].value;
      cum += length;
      this.offsets.push(cum);
    }
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
    return mt.getSequence(this.cs);
  }

  // Returns the sequences pointed to by all items[i], s.t. start <= i < end, and returns the
  // concatentation as one long composite sequence
  getCompositeChildSequence(start: number, length: number):
      Promise<IndexedSequence> {
    const childrenP = [];
    for (let i = start; i < start + length; i++) {
      childrenP.push(this.items[i].getSequence(this.cs));
    }

    return Promise.all(childrenP).then(children => {
      const items = [];
      children.forEach(child => items.push(...child.items));
      return children[0].isMeta ? new IndexedMetaSequence(this.cs, this.type, items)
        : new IndexedSequence(this.cs, this.type, items);
    });
  }

  getOffset(idx: number): number {
    return this.offsets[idx] - 1;
  }
}

export class OrderedMetaSequence<K: valueOrPrimitive> extends OrderedSequence<K, MetaTuple<K>> {
  constructor(cs: ?ChunkStore, type: Type, items: Array<MetaTuple>) {
    super(cs, type, items);
    this.isMeta = true;
  }

  getChildSequence(idx: number): Promise<?Sequence> {
    if (!this.isMeta) {
      return Promise.resolve(null);
    }

    const mt = this.items[idx];
    return mt.getSequence(this.cs);
  }

  getKey(idx: number): K {
    return this.items[idx].value;
  }
}

export function newMetaSequenceFromData(cs: ChunkStore, type: Type, tuples: Array<MetaTuple>):
    MetaSequence {
  switch (type.kind) {
    case Kind.Map:
    case Kind.Set:
      return new OrderedMetaSequence(cs, type, tuples);
    case Kind.List:
      return new IndexedMetaSequence(cs, type, tuples);
    case Kind.Blob:
      throw new Error('Not implemented');
    default:
      throw new Error('Not reached');
  }
}

const indexedSequenceIndexType = makePrimitiveType(Kind.Uint64);

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
        return makeCompoundType(Kind.Ref, makePrimitiveType(Kind.Value));
      }
    }
    case Kind.Blob:
    case Kind.List:
      return indexedSequenceIndexType;
  }

  throw new Error('Not reached');
}

export function newOrderedMetaSequenceChunkFn(t: Type, cs: ?ChunkStore = null): makeChunkFn {
  return (tuples: Array<MetaTuple>) => {
    const meta = new OrderedMetaSequence(cs, t, tuples);
    const lastValue = tuples[tuples.length - 1].value;
    return [new MetaTuple(meta, lastValue), meta];
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

export function newIndexedMetaSequenceChunkFn(t: Type, cs: ?ChunkStore = null): makeChunkFn {
  return (tuples: Array<MetaTuple>) => {
    const sum = tuples.reduce((l, mt) => l + mt.value, 0);
    const meta = new IndexedMetaSequence(cs, t, tuples);
    return [new MetaTuple(meta, sum), meta];
  };
}

export function newIndexedMetaSequenceBoundaryChecker(): BoundaryChecker<MetaTuple> {
  return new BuzHashBoundaryChecker(objectWindowSize, sha1Size, objectPattern,
    (mt: MetaTuple) => mt.ref.digest
  );
}

