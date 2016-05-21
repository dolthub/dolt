// @flow

import BuzHashBoundaryChecker from './buzhash-boundary-checker.js';
import type {BoundaryChecker, makeChunkFn} from './sequence-chunker.js';
import type {ValueReader} from './value-store.js';
import type {Splice} from './edit-distance.js';
import type Value from './value.js'; // eslint-disable-line no-unused-vars
import type {AsyncIterator} from './async-iterator.js';
import {chunkSequence, chunkSequenceSync} from './sequence-chunker.js';
import {Collection} from './collection.js';
import {IndexedSequence, IndexedSequenceIterator} from './indexed-sequence.js';
import {diff} from './indexed-sequence-diff.js';
import {getHashOfValue} from './get-hash.js';
import {invariant} from './assert.js';
import {MetaTuple, newIndexedMetaSequenceBoundaryChecker,
  newIndexedMetaSequenceChunkFn} from './meta-sequence.js';
import {sha1Size} from './hash.js';
import RefValue from './ref-value.js';
import {getValueChunks} from './sequence.js';
import {makeListType, makeUnionType, getTypeOfValue} from './type.js';
import {equals} from './compare.js';
import {Kind} from './noms-kind.js';

const listWindowSize = 64;
const listPattern = ((1 << 6) | 0) - 1;

function newListLeafChunkFn<T: Value>(vr: ?ValueReader): makeChunkFn {
  return (items: Array<T>) => {
    const list = newListFromSequence(newListLeafSequence(vr, items));
    const mt = new MetaTuple(new RefValue(list), items.length, items.length, list);
    return [mt, list];
  };
}

function newListLeafBoundaryChecker<T: Value>(): BoundaryChecker<T> {
  return new BuzHashBoundaryChecker(listWindowSize, sha1Size, listPattern,
    (v: T) => getHashOfValue(v).digest
  );
}

export default class List<T: Value> extends Collection<IndexedSequence> {
  constructor(values: Array<T> = []) {
    const self = chunkSequenceSync(
        values,
        newListLeafChunkFn(null),
        newIndexedMetaSequenceChunkFn(Kind.List, null),
        newListLeafBoundaryChecker(),
        newIndexedMetaSequenceBoundaryChecker);
    super(self.sequence);
  }

  async get(idx: number): Promise<T> {
    invariant(idx >= 0 && idx < this.length);
    return this.sequence.newCursorAt(idx).then(cursor => cursor.getCurrent());
  }

  splice(idx: number, deleteCount: number, ...insert: Array<T>): Promise<List<T>> {
    const vr = this.sequence.vr;
    return this.sequence.newCursorAt(idx).then(cursor =>
      chunkSequence(cursor, insert, deleteCount, newListLeafChunkFn(vr),
                    newIndexedMetaSequenceChunkFn(Kind.List, vr),
                    newListLeafBoundaryChecker(),
                    newIndexedMetaSequenceBoundaryChecker));
  }

  insert(idx: number, ...values: Array<T>): Promise<List<T>> {
    return this.splice(idx, 0, ...values);
  }

  remove(start: number, end: number): Promise<List<T>> {
    return this.splice(start, end - start);
  }

  append(...values: Array<T>): Promise<List<T>> {
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

  diff(last: List<T>, loadLimit: number = -1): Promise<Array<Splice>> {
    invariant(equals(this.type, last.type));

    if (equals(this, last)) {
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

export function newListFromSequence<T: Value>(sequence: IndexedSequence): List<T> {
  const list = Object.create(List.prototype);
  list._hash = null; // ValueBase
  list.sequence = sequence;
  return list;
}

export class ListLeafSequence<T: Value> extends IndexedSequence<T> {
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

export function newListLeafSequence<T: Value>(vr: ?ValueReader, items: T[]):
    ListLeafSequence<T> {
  const t = makeListType(makeUnionType(items.map(getTypeOfValue)));
  return new ListLeafSequence(vr, t, items);
}

function clampIndex(idx: number, length: number): number {
  if (idx > length) {
    return length;
  }

  return idx < 0 ? Math.max(0, length + idx) : idx;
}
