// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {AsyncIterator} from './async-iterator.js';
import type {AsyncIteratorResult} from './async-iterator.js';
import type Value from './value.js'; // eslint-disable-line no-unused-vars
import {invariant, notNull} from './assert.js';
import {compare} from './compare.js';
import search from './binary-search.js';
import Sequence, {SequenceCursor} from './sequence.js';
import {ValueBase} from './value.js';

export class OrderedSequence<K: Value, T> extends Sequence<T> {
  // Returns:
  //   -null, if sequence is empty.
  //   -null, if all values in sequence are < key.
  //   -cursor positioned at
  //      -first value, if |key| is null
  //      -first value >= |key|
  async newCursorAt(key: ?K, forInsertion: boolean = false, last: boolean = false):
      Promise<OrderedSequenceCursor> {
    let cursor: ?OrderedSequenceCursor = null;
    let sequence: ?OrderedSequence = this;

    while (sequence) {
      cursor = new OrderedSequenceCursor(cursor, sequence, last ? -1 : 0);
      if (key !== null && key !== undefined) {
        const lastPositionIfNotfound = forInsertion && sequence.isMeta;
        if (!cursor._seekTo(key, lastPositionIfNotfound)) {
          return cursor; // invalid
        }
      }

      sequence = await cursor.getChildSequence();
    }

    return notNull(cursor);
  }

  /**
   * Gets the key used for ordering the sequence at index |idx|.
   */
  getKey(idx: number): K { // eslint-disable-line no-unused-vars
    throw new Error('override');
  }

  /**
   * Returns true if the item in this sequence at |idx| is equal to |other|.
   */
  equalsAt(idx: number, other: any): boolean { // eslint-disable-line no-unused-vars
    throw new Error('override');
  }
}

export class OrderedSequenceCursor<T, K: Value> extends
    SequenceCursor<T, OrderedSequence> {
  getCurrentKey(): K {
    invariant(this.idx >= 0 && this.idx < this.length);
    return this.sequence.getKey(this.idx);
  }

  clone(): OrderedSequenceCursor<T, K> {
    return new OrderedSequenceCursor(this.parent && this.parent.clone(), this.sequence, this.idx);
  }

  // Moves the cursor to the first value in sequence >= key and returns true.
  // If none exists, returns false.
  _seekTo(key: K, lastPositionIfNotfound: boolean = false): boolean {
    this.idx = search(this.length, getSearchFunction(this.sequence, key));

    if (this.idx === this.length && lastPositionIfNotfound) {
      invariant(this.idx > 0);
      this.idx--;
    }

    return this.idx < this.length;
  }

  async advanceTo(key: K): Promise<boolean> {
    if (!this.valid) {
      throw new Error('Invalid Cursor');
    }

    if (this._seekTo(key)) {
      return true;
    }

    if (!this.parent) {
      return false;
    }

    const p = this.parent;
    invariant(p instanceof OrderedSequenceCursor);
    const old = p.getCurrent();
    if (!await p.advanceTo(key)) {
      return false;
    }

    this.idx = 0;
    if (old !== p.getCurrent()) {
      await this.sync();
    }

    invariant(this._seekTo(key));
    return true;
  }
}

export class OrderedSequenceIterator<T, K: Value> extends AsyncIterator<T> {
  _iterator: Promise<AsyncIterator<T>>;

  constructor(cursorP: Promise<OrderedSequenceCursor<T, K>>) {
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

function getSearchFunction(sequence: OrderedSequence, key: Value):
    (i: number) => number {
  if (sequence.isMeta) {
    const keyRef = key instanceof ValueBase ? key.hash : null;
    return i => {
      const sk = sequence.getKey(i);
      if (sk instanceof ValueBase) {
        if (keyRef) {
          return sk.targetHash.compare(keyRef);
        }
        return 1;
      }
      return compare(sk, key);
    };
  }
  return i => compare(sequence.getKey(i), key);
}
