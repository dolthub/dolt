// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {AsyncIterator} from './async-iterator.js';
import type {AsyncIteratorResult} from './async-iterator.js';
import {notNull} from './assert.js';
import Sequence, {search, SequenceCursor} from './sequence.js';

export class IndexedSequence<T> extends Sequence<T> {
  getOffset(idx: number): number { // eslint-disable-line no-unused-vars
    throw new Error('override');
  }

  async newCursorAt(idx: number): Promise<IndexedSequenceCursor> {
    let cursor: ?IndexedSequenceCursor = null;
    let sequence: ?IndexedSequence = this;

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

export class IndexedSequenceCursor<T> extends SequenceCursor<T, IndexedSequence> {
  advanceToOffset(idx: number): number {
    this.idx = search(this.length, (i: number) => this.sequence.getOffset(i) - idx);

    if (this.sequence.isMeta && this.idx === this.length) {
      this.idx = this.length - 1;
    }

    return this.idx > 0 ? this.sequence.getOffset(this.idx - 1) + 1 : 0;
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
