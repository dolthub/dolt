// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import Sequence, {SequenceCursor} from './sequence.js';
import search from './binary-search.js';
import type {AsyncIteratorResult} from './async-iterator.js';
import type {EqualsFn} from './edit-distance.js';
import {AsyncIterator} from './async-iterator.js';
import {equals} from './compare.js';
import {notNull} from './assert.js';

export class IndexedSequence<T> extends Sequence<T> {
  cumulativeNumberOfLeaves(idx: number): number { // eslint-disable-line no-unused-vars
    throw new Error('override');
  }

  getCompareFn(other: IndexedSequence<any>): EqualsFn {
    return (idx: number, otherIdx: number) =>
      // $FlowIssue: Does not realize that these are Values.
      equals(this.items[idx], other.items[otherIdx]);
  }

  async newCursorAt(idx: number): Promise<IndexedSequenceCursor<any>> {
    let cursor: ?IndexedSequenceCursor<any> = null;
    let sequence: ?IndexedSequence<any> = this;

    while (sequence) {
      cursor = new IndexedSequenceCursor(cursor, sequence, 0);
      idx -= cursor.advanceToOffset(idx);
      sequence = await cursor.getChildSequence();
    }

    return notNull(cursor);
  }

  range(start: number, end: number): Promise<Array<T>> { // eslint-disable-line no-unused-vars
    throw new Error('override');
  }
}

export class IndexedSequenceCursor<T> extends SequenceCursor<T, IndexedSequence<any>> {
  advanceToOffset(idx: number): number {
    this.idx = search(this.length, (i: number) => idx < this.sequence.cumulativeNumberOfLeaves(i));

    if (this.sequence.isMeta && this.idx === this.length) {
      this.idx = this.length - 1;
    }

    return this.idx > 0 ? this.sequence.cumulativeNumberOfLeaves(this.idx - 1) : 0;
  }

  clone(): IndexedSequenceCursor<T> {
    return new IndexedSequenceCursor(this.parent && this.parent.clone(), this.sequence, this.idx);
  }
}

export class IndexedSequenceIterator<T> extends AsyncIterator<T> {
  _iterator: Promise<AsyncIterator<T>>;

  constructor(cursorP: Promise<IndexedSequenceCursor<T>>) {
    super();
    this._iterator = cursorP.then(cur => cur.iterator());
  }

  next(): Promise<AsyncIteratorResult<T>> {
    return this._iterator.then(it => it.next());
  }

  return(): Promise<AsyncIteratorResult<T>> {
    return this._iterator.then(it => it.return());
  }
}
