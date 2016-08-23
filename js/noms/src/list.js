// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import type {makeChunkFn} from './sequence-chunker.js';
import type {ValueReader, ValueReadWriter} from './value-store.js';
import type {Splice} from './edit-distance.js';
import type Value from './value.js'; // eslint-disable-line no-unused-vars
import type {AsyncIterator} from './async-iterator.js';
import {default as SequenceChunker, chunkSequence, chunkSequenceSync} from './sequence-chunker.js';
import Collection from './collection.js';
import {IndexedSequence, IndexedSequenceIterator} from './indexed-sequence.js';
import {diff} from './indexed-sequence-diff.js';
import {invariant} from './assert.js';
import {
  OrderedKey,
  newIndexedMetaSequenceChunkFn,
} from './meta-sequence.js';
import Ref from './ref.js';
import {getValueChunks} from './sequence.js';
import {makeListType, makeUnionType, getTypeOfValue} from './type.js';
import {equals} from './compare.js';
import {Kind} from './noms-kind.js';
import {DEFAULT_MAX_SPLICE_MATRIX_SIZE} from './edit-distance.js';
import {hashValueBytes} from './rolling-value-hasher.js';

function newListLeafChunkFn<T: Value>(vr: ?ValueReader): makeChunkFn<any, any> {
  return (items: Array<T>) => {
    const seq = newListLeafSequence(vr, items);
    const list = List.fromSequence(seq);
    const key = new OrderedKey(items.length);
    return [list, key, items.length];
  };
}

export default class List<T: Value> extends Collection<IndexedSequence<any>> {
  constructor(values: Array<T> = []) {
    const seq = chunkSequenceSync(
        values,
        newListLeafChunkFn(null),
        newIndexedMetaSequenceChunkFn(Kind.List, null, null),
        hashValueBytes);
    invariant(seq instanceof IndexedSequence);
    super(seq);
  }

  async get(idx: number): Promise<T> {
    invariant(idx >= 0 && idx < this.length);
    return this.sequence.newCursorAt(idx).then(cursor => cursor.getCurrent());
  }

  splice(idx: number, deleteCount: number, ...insert: Array<T>): Promise<List<T>> {
    const vr = this.sequence.vr;
    return this.sequence.newCursorAt(idx).then(cursor =>
      chunkSequence(cursor, vr, insert, deleteCount, newListLeafChunkFn(vr),
                    newIndexedMetaSequenceChunkFn(Kind.List, vr, null),
                    hashValueBytes)).then(s => List.fromSequence(s));
  }

  insert(idx: number, ...values: Array<T>): Promise<List<T>> {
    return this.splice(idx, 0, ...values);
  }

  remove(idx: number): Promise<List<T>> {
    return this.splice(idx, 1);
  }

  append(...values: Array<T>): Promise<List<T>> {
    return this.splice(this.length, 0, ...values);
  }

  async forEach(cb: (v: T, i: number) => ?Promise<void>): Promise<void> {
    const cursor = await this.sequence.newCursorAt(0);
    const promises = [];
    return cursor.iter((v, i) => {
      promises.push(cb(v, i));
      return false;
    }).then(() => Promise.all(promises)).then(() => void 0);
  }

  iterator(): AsyncIterator<T> {
    return new IndexedSequenceIterator(this.sequence.newCursorAt(0));
  }

  iteratorAt(i: number): AsyncIterator<T> {
    return new IndexedSequenceIterator(this.sequence.newCursorAt(i));
  }

  diff(last: List<T>,
       maxSpliceMatrixSize: number = DEFAULT_MAX_SPLICE_MATRIX_SIZE): Promise<Array<Splice>> {
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

    return Promise.all([last.sequence.newCursorAt(0), this.sequence.newCursorAt(0)]).then(cursors =>
        diff(last.sequence, cursors[0].depth, 0, this.sequence, cursors[1].depth, 0,
             maxSpliceMatrixSize));
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

export class ListLeafSequence<T: Value> extends IndexedSequence<T> {
  get chunks(): Array<Ref<any>> {
    return getValueChunks(this.items);
  }

  cumulativeNumberOfLeaves(idx: number): number {
    return idx + 1;
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

type ListWriterState = 'writable' | 'closed';

export class ListWriter<T: Value> {
  _state: ListWriterState;
  _list: ?Promise<List<T>>;
  _chunker: SequenceChunker<T, ListLeafSequence<T>>;
  _vrw: ?ValueReadWriter;

  constructor(vrw: ?ValueReadWriter) {
    this._state = 'writable';
    this._chunker = new SequenceChunker(null, vrw, vrw, newListLeafChunkFn(vrw),
        newIndexedMetaSequenceChunkFn(Kind.List, vrw, vrw), hashValueBytes);
    this._vrw = vrw;
  }

  write(item: T) {
    assert(this._state === 'writable');
    this._chunker.append(item);
  }

  close() {
    assert(this._state === 'writable');
    this._list = this._chunker.done(this._vrw).then(seq => List.fromSequence(seq));
    this._state = 'closed';
  }

  get list(): Promise<List<any>> {
    assert(this._state === 'closed');
    invariant(this._list);
    return this._list;
  }
}

function assert(v: any) {
  if (!v) {
    throw new TypeError('Invalid usage of ListWriter');
  }
}
