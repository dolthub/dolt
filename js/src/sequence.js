// @flow

import type DataStore from './data-store.js';
import {invariant, notNull} from './assert.js';
import {AsyncIterator} from './async-iterator.js';
import type {AsyncIteratorResult} from './async-iterator.js';
import type {Type} from './type.js';
import {ValueBase} from './value.js';

export class Sequence<T> extends ValueBase {
  ds: ?DataStore;
  _type: Type;
  items: Array<T>;

  constructor(ds: ?DataStore, type: Type, items: Array<T>) {
    super();
    this.ds = ds;
    this._type = type;
    this.items = items;
  }

  get type(): Type {
    return this._type;
  }

  get isMeta(): boolean {
    return false;
  }

  getChildSequence(idx: number): Promise<?Sequence> { // eslint-disable-line no-unused-vars
    return Promise.resolve(null);
  }

  get length(): number {
    return this.items.length;
  }
}

export class SequenceCursor<T, S: Sequence> {
  parent: ?SequenceCursor;
  sequence: S;
  idx: number;

  constructor(parent: ?SequenceCursor, sequence: S, idx: number) {
    this.parent = parent;
    this.sequence = sequence;
    this.idx = idx;
    if (this.idx < 0) {
      this.idx = Math.max(0, this.sequence.length + this.idx);
    }
  }

  clone(): SequenceCursor<T, S> {
    throw new Error('override');
  }

  get length(): number {
    return this.sequence.length;
  }

  getItem(idx: number): T {
    return this.sequence.items[idx];
  }

  sync(): Promise<void> {
    invariant(this.parent);
    return this.parent.getChildSequence().then(p => {
      this.sequence = notNull(p);
    });
  }

  getChildSequence(): Promise<?S> {
    return this.sequence.getChildSequence(this.idx);
  }

  getCurrent(): T {
    invariant(this.valid);
    return this.getItem(this.idx);
  }

  get valid(): boolean {
    return this.idx >= 0 && this.idx < this.length;
  }

  get indexInChunk(): number {
    return this.idx;
  }

  get depth(): number {
    return 1 + (this.parent ? this.parent.depth : 0);
  }

  advance(allowPastEnd: boolean = true): Promise<boolean> {
    return this._advanceMaybeAllowPastEnd(allowPastEnd);
  }

  /**
   * Advances the cursor within the current chunk.
   *
   * Returns true if the cursor advanced to a valid position within this chunk, false if not.
   *
   * If |allowPastEnd| is true, the cursor is allowed to advance one index past the end of the chunk
   * (an invalid position, so the return value will be false).
   */
  advanceLocal(allowPastEnd: boolean): boolean {
    if (this.idx === this.length) {
      return false;
    }

    if (this.idx < this.length - 1) {
      this.idx++;
      return true;
    }

    if (allowPastEnd) {
      this.idx++;
    }

    return false;
  }

  /**
   * Returns true if the cursor can advance within the current chunk to a valid position.
   */
  canAdvanceLocal(): boolean {
    return this.idx < this.length - 1;
  }

  async _advanceMaybeAllowPastEnd(allowPastEnd: boolean): Promise<boolean> {
    if (this.idx === this.length) {
      return Promise.resolve(false);
    }

    if (this.advanceLocal(allowPastEnd)) {
      return Promise.resolve(true);
    }

    if (this.parent && await this.parent._advanceMaybeAllowPastEnd(false)) {
      await this.sync();
      this.idx = 0;
      return true;
    }

    return false;
  }

  advanceChunk(): Promise<boolean> {
    this.idx = this.length - 1;
    return this._advanceMaybeAllowPastEnd(true);
  }

  retreat(): Promise<boolean> {
    return this._retreatMaybeAllowBeforeStart(true);
  }

  async _retreatMaybeAllowBeforeStart(allowBeforeStart: boolean): Promise<boolean> {
    // TODO: Factor this similar to advance().
    if (this.idx > 0) {
      this.idx--;
      return true;
    }
    if (this.idx === -1) {
      return false;
    }
    invariant(this.idx === 0);
    if (this.parent && await this.parent._retreatMaybeAllowBeforeStart(false)) {
      await this.sync();
      this.idx = this.length - 1;
      return true;
    }

    if (allowBeforeStart) {
      this.idx--;
    }

    return false;
  }

  async maxNPrevItems(n: number): Promise<Array<T>> {
    const prev = [];
    const retreater = this.clone();
    for (let i = 0; i < n && await retreater.retreat(); i++) {
      prev.push(retreater.getCurrent());
    }

    prev.reverse();
    return prev;
  }

  async iter(cb: (v: T, i: number) => boolean): Promise<void> {
    let idx = 0;
    while (this.valid) {
      if (cb(this.getItem(this.idx), idx++)) {
        return;
      }
      this.advanceLocal(false) || await this.advance();
    }
  }

  iterator(): AsyncIterator<T> {
    return new SequenceIterator(this);
  }
}

export class SequenceIterator<T, S: Sequence> extends AsyncIterator<T> {
  _cursor: SequenceCursor<T, S>;
  _advance: Promise<boolean>;
  _closed: boolean;

  constructor(cursor: SequenceCursor<T, S>) {
    super();
    this._cursor = cursor;
    this._advance = Promise.resolve(cursor.valid);
    this._closed = false;
  }

  next(): Promise<AsyncIteratorResult<T>> {
    return this._safeAdvance(success => {
      if (!success || this._closed) {
        return {done: true};
      }
      const cur = this._cursor;
      const value = cur.getCurrent();
      if (!cur.advanceLocal(false)) {
        // Advance the cursor to the next chunk, invalidating any in-progress calls to next(), since
        // they were wrapped in |_safeAdvance|. They will just try again. This works because the
        // ordering of Promise callbacks is guaranteed to be in .then() order.
        this._advance = cur.advance();
      }
      return {done: false, value};
    });
  }

  return(): Promise<AsyncIteratorResult<T>> {
    return this._safeAdvance(() => {
      this._closed = true;
      return {done: true};
    });
  }

  // Wraps |_advance|.then() with the guarantee that |_advance| hasn't changed since running .then()
  // and the callback being run.
  _safeAdvance(fn: (success: boolean) => AsyncIteratorResult<T> | Promise<AsyncIteratorResult<T>>)
              :Promise<AsyncIteratorResult<T>> {
    const run = advance =>
      advance.then(success => {
        if (advance !== this._advance) {
          return run(this._advance);
        }
        return fn(success);
      });
    return run(this._advance);
  }
}

// Translated from golang source (https://golang.org/src/sort/search.go?s=2249:2289#L49)
export function search(n: number, f: (i: number) => boolean): number {
  // Define f(-1) == false and f(n) == true.
  // Invariant: f(i-1) == false, f(j) == true.
  let i = 0;
  let j = n;
  while (i < j) {
    const h = i + (((j - i) / 2) | 0); // avoid overflow when computing h
    // i ≤ h < j
    if (!f(h)) {
      i = h + 1; // preserves f(i-1) == false
    } else {
      j = h; // preserves f(j) == true
    }
  }

  // i == j, f(i-1) == false, and f(j) (= f(i)) == true  =>  answer is i.
  return i;
}
