  // @flow

import BuzHashBoundaryChecker from './buzhash_boundary_checker.js';
import type {BoundaryChecker, makeChunkFn} from './sequence_chunker.js';
import type {ChunkStore} from './chunk_store.js';
import type {valueOrPrimitive} from './value.js'; // eslint-disable-line no-unused-vars
import {AsyncIterator} from './async_iterator.js';
import {chunkSequence} from './sequence_chunker.js';
import {Collection} from './collection.js';
import {getRefOfValueOrPrimitive} from './get_ref.js';
import {IndexedSequence, IndexedSequenceIterator} from './indexed_sequence.js';
import {invariant} from './assert.js';
import {MetaTuple, newIndexedMetaSequenceBoundaryChecker,
  newIndexedMetaSequenceChunkFn} from './meta_sequence.js';
import {sha1Size} from './ref.js';
import {Type} from './type.js';

const listWindowSize = 64;
const listPattern = ((1 << 6) | 0) - 1;

function newListLeafChunkFn<T: valueOrPrimitive>(t: Type): makeChunkFn {
  return (items: Array<T>) => {
    const listLeaf = new ListLeafSequence(t, items);
    const mt = new MetaTuple(listLeaf, items.length);
    return [mt, listLeaf];
  };
}

function newListLeafBoundaryChecker<T: valueOrPrimitive>(t: Type): BoundaryChecker<T> {
  return new BuzHashBoundaryChecker(listWindowSize, sha1Size, listPattern,
    (v: T) => getRefOfValueOrPrimitive(v, t.elemTypes[0]).digest
  );
}

export function newList<T: valueOrPrimitive>(cs: ChunkStore, type: Type, values: Array<T>):
    Promise<NomsList<T>> {
  return chunkSequence(null, values, 0, newListLeafChunkFn(type),
                       newIndexedMetaSequenceChunkFn(type),
                       newListLeafBoundaryChecker(type),
                       newIndexedMetaSequenceBoundaryChecker)
  .then((seq: IndexedSequence) => new NomsList(cs, type, seq));
}

export class NomsList<T: valueOrPrimitive> extends Collection<IndexedSequence> {
  async get(idx: number): Promise<T> {
    // TODO (when |length| works) invariant(idx < this.length, idx + ' >= ' + this.length);
    const cursor = await this.sequence.newCursorAt(this.cs, idx);
    return cursor.getCurrent();
  }

  async splice(idx: number, insert: Array<T>, remove: number): Promise<NomsList<T>> {
    const cursor = await this.sequence.newCursorAt(this.cs, idx);
    const type = this.type;
    const seq = await chunkSequence(cursor, insert, remove, newListLeafChunkFn(type),
                                    newIndexedMetaSequenceChunkFn(type),
                                    newListLeafBoundaryChecker(type),
                                    newIndexedMetaSequenceBoundaryChecker);
    invariant(seq instanceof IndexedSequence);
    return new NomsList(this.cs, type, seq);
  }

  insert(idx: number, values: Array<T>): Promise<NomsList<T>> {
    return this.splice(idx, values, 0);
  }

  remove(start: number, end: number): Promise<NomsList<T>> {
    return this.splice(start, [], end - start);
  }

  append(values: Array<T>): Promise<NomsList<T>> {
    return this.splice(this.length, values, 0);
  }

  async forEach(cb: (v: T, i: number) => void): Promise<void> {
    const cursor = await this.sequence.newCursorAt(this.cs, 0);
    return cursor.iter((v, i) => {
      cb(v, i);
      return false;
    });
  }

  iterator(): AsyncIterator<T> {
    return new IndexedSequenceIterator(this.sequence.newCursorAt(this.cs, 0));
  }

  iteratorAt(i: number): AsyncIterator<T> {
    return new IndexedSequenceIterator(this.sequence.newCursorAt(this.cs, i));
  }

  get length(): number {
    if (this.sequence instanceof ListLeafSequence) {
      return this.sequence.items.length;
    }
    return this.sequence.items.reduce((v, tuple) => v + tuple.value, 0);
  }
}

export class ListLeafSequence<T: valueOrPrimitive> extends IndexedSequence<T> {
  getOffset(idx: number): number {
    return idx;
  }
}
