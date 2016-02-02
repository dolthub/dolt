// @flow

import {AsyncIterator, AsyncIteratorResult} from './async_iterator.js';
import type {ChunkStore} from './chunk_store.js';
import type {valueOrPrimitive} from './value.js'; // eslint-disable-line no-unused-vars
import {invariant, notNull} from './assert.js';
import {less} from './value.js';
import {search, Sequence, SequenceCursor} from './sequence.js';

export class OrderedSequence<K: valueOrPrimitive, T> extends Sequence<T> {
  // Returns:
  //   -null, if sequence is empty.
  //   -null, if all values in sequence are < key.
  //   -cursor positioned at
  //      -first value, if |key| is null
  //      -first value >= |key|
  async newCursorAt(cs: ChunkStore, key: ?K): Promise<OrderedSequenceCursor> {
    let cursor: ?OrderedSequenceCursor = null;
    let sequence: ?OrderedSequence = this;

    while (sequence) {
      cursor = new OrderedSequenceCursor(cs, cursor, sequence, 0);
      if (key) {
        if (!cursor._seekTo(key)) {
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

  // Moves the cursor to the first value in sequence >= key and returns true.
  // If none exists, returns false.
  _seekTo(key: K): boolean {
    this.idx = search(this.length, (i: number) => !less(this.sequence.getKey(i), key));

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
  _cursorP: Promise<OrderedSequenceCursor<T, K>>;
  _iterator: ?AsyncIterator<T>;

  constructor(cursorP: Promise<OrderedSequenceCursor<T, K>>) {
    super();
    this._cursorP = cursorP;
  }

  async _ensureIterator(): Promise<AsyncIterator<T>> {
    if (!this._iterator) {
      this._iterator = (await this._cursorP).iterator();
    }
    return this._iterator;
  }

  next(): Promise<AsyncIteratorResult<T>> {
    return this._ensureIterator().then(it => it.next());
  }

  return(): Promise<AsyncIteratorResult<T>> {
    return this._ensureIterator().then(it => it.return());
  }
}
