// @flow

import type {ChunkStore} from './chunk_store.js';
import {invariant, notNull} from './assert.js';
import {AsyncIterator, AsyncIteratorResult} from './async_iterator.js';
import {Type} from './type.js';
import {ValueBase} from './value.js';

export class Sequence<T> extends ValueBase {
  cs: ?ChunkStore;
  items: Array<T>;
  isMeta: boolean;

  constructor(cs: ?ChunkStore, type: Type, items: Array<T>) {
    super(type);

    this.cs = cs;
    this.items = items;
    this.isMeta = false;
  }

  getChildSequence(idx: number): // eslint-disable-line no-unused-vars
      Promise<?Sequence> {
    return Promise.resolve(null);
  }

  get length(): number {
    return this.items.length;
  }
}

export class SequenceCursor<T, S:Sequence> {
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
    return this.sequence.items.length;
  }

  getItem(idx: number): T {
    return this.sequence.items[idx];
  }

  async sync(): Promise<void> {
    invariant(this.parent);
    this.sequence = notNull(await this.parent.getChildSequence());
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

  advance(): Promise<boolean> {
    return this._advanceMaybeAllowPastEnd(true);
  }

  advanceLocal(): boolean {
    if (this.idx < this.length - 1) {
      this.idx++;
      return true;
    }

    return false;
  }

  async _advanceMaybeAllowPastEnd(allowPastEnd: boolean): Promise<boolean> {
    if (this.idx < this.length - 1) {
      this.idx++;
      return true;
    }

    if (this.idx === this.length) {
      return false;
    }

    if (this.parent && (await this.parent._advanceMaybeAllowPastEnd(false))) {
      await this.sync();
      this.idx = 0;
      return true;
    }
    if (allowPastEnd) {
      this.idx++;
    }

    return false;
  }

  retreat(): Promise<boolean> {
    return this._retreatMaybeAllowBeforeStart(true);
  }

  async _retreatMaybeAllowBeforeStart(allowBeforeStart: boolean): Promise<boolean> {
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
      this.advanceLocal() || await this.advance();
    }
  }

  iterator(): AsyncIterator<T> {
    return new SequenceIterator(this);
  }
}

export class SequenceIterator<T, S:Sequence> extends AsyncIterator<T> {
  _cursor: SequenceCursor<T, S>;
  _nextP: Promise<AsyncIteratorResult<T>>;
  _closed: boolean;

  constructor(cursor: SequenceCursor<T, S>) {
    super();
    this._cursor = cursor;
    this._closed = false;
    this._nextP = Promise.resolve(
        cursor.valid ? {done: false, value: cursor.getCurrent()} : {done: true});
  }

  async next(): Promise<AsyncIteratorResult<T>> {
    if (this._closed) {
      return {done: true};
    }
    const next = await this._nextP;
    if (this._cursor.advanceLocal()) {
      this._nextP = Promise.resolve({done: false, value: this._cursor.getCurrent()});
    } else {
      this._nextP = this._cursor.advance().then(
          success => success ? {done: false, value: this._cursor.getCurrent()} : {done: true});
    }
    return next;
  }

  return(): Promise<AsyncIteratorResult<T>> {
    this._closed = true;
    return this.next();
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
