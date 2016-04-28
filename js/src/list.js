  // @flow

import BuzHashBoundaryChecker from './buzhash-boundary-checker.js';
import type {BoundaryChecker, makeChunkFn} from './sequence-chunker.js';
import type DataStore from './data-store.js';
import type {Splice} from './edit-distance.js';
import type {valueOrPrimitive} from './value.js'; // eslint-disable-line no-unused-vars
import type {AsyncIterator} from './async-iterator.js';
import {chunkSequence} from './sequence-chunker.js';
import {Collection} from './collection.js';
import {IndexedSequence, IndexedSequenceIterator} from './indexed-sequence.js';
import {diff} from './indexed-sequence-diff.js';
import {getRefOfValue} from './get-ref.js';
import {invariant} from './assert.js';
import {MetaTuple, newIndexedMetaSequenceBoundaryChecker,
  newIndexedMetaSequenceChunkFn} from './meta-sequence.js';
import {sha1Size} from './ref.js';
import RefValue from './ref-value.js';
import {getValueChunks} from './sequence.js';
import {listOfValueType, Type} from './type.js';

const listWindowSize = 64;
const listPattern = ((1 << 6) | 0) - 1;

function newListLeafChunkFn<T: valueOrPrimitive>(t: Type, ds: ?DataStore = null): makeChunkFn {
  return (items: Array<T>) => {
    const listLeaf = new ListLeafSequence(ds, t, items);
    const mt = new MetaTuple(listLeaf, items.length, items.length);
    return [mt, listLeaf];
  };
}

function newListLeafBoundaryChecker<T: valueOrPrimitive>(t: Type): BoundaryChecker<T> {
  return new BuzHashBoundaryChecker(listWindowSize, sha1Size, listPattern,
    (v: T) => getRefOfValue(v, t.elemTypes[0]).digest
  );
}

export function newList<T: valueOrPrimitive>(values: Array<T>, type: Type = listOfValueType):
    Promise<NomsList<T>> {
  return chunkSequence(null, values, 0, newListLeafChunkFn(type),
                       newIndexedMetaSequenceChunkFn(type),
                       newListLeafBoundaryChecker(type),
                       newIndexedMetaSequenceBoundaryChecker)
  .then((seq: IndexedSequence) => new NomsList(type, seq));
}

export class NomsList<T: valueOrPrimitive> extends Collection<IndexedSequence> {
  async get(idx: number): Promise<T> {
    // TODO (when |length| works) invariant(idx < this.length, idx + ' >= ' + this.length);
    const cursor = await this.sequence.newCursorAt(idx);
    return cursor.getCurrent();
  }

  async splice(idx: number, deleteCount: number, ...insert: Array<T>): Promise<NomsList<T>> {
    const cursor = await this.sequence.newCursorAt(idx);
    const ds = this.sequence.ds;
    const type = this.type;
    const seq = await chunkSequence(cursor, insert, deleteCount, newListLeafChunkFn(type, ds),
                                    newIndexedMetaSequenceChunkFn(type, ds),
                                    newListLeafBoundaryChecker(type),
                                    newIndexedMetaSequenceBoundaryChecker);
    invariant(seq instanceof IndexedSequence);
    return new NomsList(type, seq);
  }

  insert(idx: number, ...values: Array<T>): Promise<NomsList<T>> {
    return this.splice(idx, 0, ...values);
  }

  remove(start: number, end: number): Promise<NomsList<T>> {
    return this.splice(start, end - start);
  }

  append(...values: Array<T>): Promise<NomsList<T>> {
    return this.splice(this.length, 0, ...values);
  }

  async forEach(cb: (v: T, i: number) => void): Promise<void> {
    const cursor = await this.sequence.newCursorAt(0);
    return cursor.iter((v, i) => {
      cb(v, i);
      return false;
    });
  }

  iterator(): AsyncIterator<T> {
    return new IndexedSequenceIterator(this.sequence.newCursorAt(0));
  }

  iteratorAt(i: number): AsyncIterator<T> {
    return new IndexedSequenceIterator(this.sequence.newCursorAt(i));
  }

  diff(last: NomsList<T>, loadLimit: number = -1): Promise<Array<Splice>> {
    invariant(this.type.equals(last.type));

    if (this.equals(last)) {
      return Promise.resolve([]); // nothing changed.
    }
    if (this.length === 0) {
      return Promise.resolve([[0, last.length, 0, 0]]); // Everything removed
    }
    if (last.length === 0) {
      return Promise.resolve([[0, 0, this.length, 0]]); // Everything added
    }

    const loadLimitArg = loadLimit === -1 ? null : {count : loadLimit};
    return Promise.all([last.sequence.newCursorAt(0), this.sequence.newCursorAt(0)]).then(cursors =>
        diff(last.sequence, cursors[0].depth, 0, this.sequence, cursors[1].depth, 0, loadLimitArg));
  }

  // $FlowIssue
  toJS(start: number = 0, end: number = this.length): Promise<Array<T>> {
    const l = this.length;
    start = clampIndex(start, l);
    end = clampIndex(end, l);
    if (start >= end) {
      return Promise.resolve([]);
    }
    return this.sequence.range(start, end);
  }

  get length(): number {
    return this.sequence.numLeaves;
  }
}

export class ListLeafSequence<T: valueOrPrimitive> extends IndexedSequence<T> {
  get chunks(): Array<RefValue> {
    return getValueChunks(this.items);
  }

  getOffset(idx: number): number {
    return idx;
  }

  range(start: number, end: number): Promise<Array<T>> {
    invariant(start >= 0 && end >= 0 && end <= this.items.length);
    return Promise.resolve(this.items.slice(start, end));
  }
}

function clampIndex(idx: number, length: number): number {
  if (idx > length) {
    return length;
  }

  return idx < 0 ? Math.max(0, length + idx) : idx;
}
