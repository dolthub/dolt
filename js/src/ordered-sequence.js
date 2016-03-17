// @flow

import {AsyncIterator} from './async-iterator.js';
import type {AsyncIteratorResult} from './async-iterator.js';
import type {valueOrPrimitive} from './value.js'; // eslint-disable-line no-unused-vars
import {invariant, isNullOrUndefined, notNull} from './assert.js';
import {less} from './value.js';
import {search, Sequence, SequenceCursor} from './sequence.js';

export class OrderedSequence<K: valueOrPrimitive, T> extends Sequence<T> {
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
      if (!isNullOrUndefined(key)) {
        const lastPositionIfNotfound = forInsertion && sequence.isMeta;
        if (!cursor._seekTo(key, lastPositionIfNotfound)) {
          return cursor; // invalid
        }
      }

      sequence = await cursor.getChildSequence();
    }

    return notNull(cursor);
  }

  getKey(idx: number): K { // eslint-disable-line no-unused-vars
    throw new Error('override');
  }
}

export class OrderedSequenceCursor<T, K: valueOrPrimitive> extends
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
    this.idx = search(this.length, (i: number) => !less(this.sequence.getKey(i), key));

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

export class OrderedSequenceIterator<T, K: valueOrPrimitive> extends AsyncIterator<T> {
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
