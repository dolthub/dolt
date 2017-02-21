// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import Sequence, {SequenceCursor} from './sequence.js';
import search from './binary-search.js';
import type {AsyncIterator, AsyncIteratorResult} from './async-iterator.js';
import {notNull} from './assert.js';

export async function newCursorAtIndex(sequence: Sequence<any>, idx: number,
    readAhead: boolean = false): Promise<IndexedSequenceCursor<any>> {
  let cursor: ?IndexedSequenceCursor<any> = null;

  while (sequence) {
    cursor = new IndexedSequenceCursor(cursor, sequence, 0, readAhead);
    idx -= cursor.advanceToOffset(idx);
    // TODO: The Sequence type needs to be cleaned up.
    // $FlowIssue: xxx
    sequence = await cursor.getChildSequence();
  }

  return notNull(cursor);
}

export class IndexedSequenceCursor<T> extends SequenceCursor<T, Sequence<any>> {
  advanceToOffset(idx: number): number {
    this.idx = search(this.length, (i: number) => idx < this.sequence.cumulativeNumberOfLeaves(i));

    if (this.sequence.isMeta && this.idx === this.length) {
      this.idx = this.length - 1;
    }

    return this.idx > 0 ? this.sequence.cumulativeNumberOfLeaves(this.idx - 1) : 0;
  }

  clone(): IndexedSequenceCursor<T> {
    return new IndexedSequenceCursor(this.parent && this.parent.clone(), this.sequence, this.idx,
        this.readAhead);
  }
}

export class IndexedSequenceIterator<T> { // AsyncIterator
  _iterator: Promise<AsyncIterator<T>>;

  constructor(cursorP: Promise<IndexedSequenceCursor<T>>) {
    this._iterator = cursorP.then(cur => cur.iterator());
  }

  next(): Promise<AsyncIteratorResult<T>> {
    return this._iterator.then(it => it.next());
  }

  return(): Promise<AsyncIteratorResult<T>> {
    return this._iterator.then(it => it.return());
  }
}
