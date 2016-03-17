// @flow

import type {Sequence} from './sequence.js'; // eslint-disable-line no-unused-vars
import {invariant, notNull} from './assert.js';
import {SequenceCursor} from './sequence.js';

export type BoundaryChecker<T> = {
  write: (item: T) => bool;
  windowSize: number;
}

export type NewBoundaryCheckerFn<T> = () => BoundaryChecker<T>;

export type makeChunkFn = (items: Array<any>) => [any, any];

export async function chunkSequence<S, T>(
  cursor: ?SequenceCursor,
  insert: Array<S>,
  remove: number,
  makeChunk: makeChunkFn,
  parentMakeChunk: makeChunkFn,
  boundaryChecker: BoundaryChecker<S>,
  newBoundaryChecker: NewBoundaryCheckerFn<T>): Promise<any> {

  const chunker = new SequenceChunker(cursor, makeChunk, parentMakeChunk, boundaryChecker,
                                      newBoundaryChecker);
  if (cursor) {
    await chunker.resume();
  }

  if (remove > 0) {
    invariant(cursor);
    for (let i = 0; i < remove; i++) {
      await chunker.skip();
    }
  }

  for (let i = 0; i < insert.length; i++) {
    chunker.append(insert[i]);
  }

  return await chunker.done();
}

export class SequenceChunker<S, T, U:Sequence, V:Sequence> {
  _cursor: ?SequenceCursor<S, U>;
  _isOnChunkBoundary: boolean;
  _parent: ?SequenceChunker<T, T, V, V>;
  _current: Array<S>;
  _makeChunk: makeChunkFn;
  _parentMakeChunk: makeChunkFn;
  _boundaryChecker: BoundaryChecker<S>;
  _newBoundaryChecker: NewBoundaryCheckerFn<T>;
  _used: boolean;

  constructor(cursor: ?SequenceCursor, makeChunk: makeChunkFn,
              parentMakeChunk: makeChunkFn,
              boundaryChecker: BoundaryChecker<S>,
              newBoundaryChecker: NewBoundaryCheckerFn<T>) {
    this._cursor = cursor;
    this._isOnChunkBoundary = false;
    this._parent = null;
    this._current = [];
    this._makeChunk = makeChunk;
    this._parentMakeChunk = parentMakeChunk;
    this._boundaryChecker = boundaryChecker;
    this._newBoundaryChecker = newBoundaryChecker;
    this._used = false;
  }

  async resume(): Promise<void> {
    const cursor = notNull(this._cursor);
    if (cursor.parent) {
      this.createParent();
      await notNull(this._parent).resume();
    }

    // TODO: Only call maxNPrevItems once.
    const prev =
      await cursor.maxNPrevItems(this._boundaryChecker.windowSize - 1);
    for (let i = 0; i < prev.length; i++) {
      this._boundaryChecker.write(prev[i]);
    }

    this._current = await cursor.maxNPrevItems(cursor.indexInChunk);
    this._used = this._current.length > 0;
  }

  append(item: S) {
    if (this._isOnChunkBoundary) {
      this.createParent();
      this.handleChunkBoundary();
      this._isOnChunkBoundary = false;
    }
    this._current.push(item);
    this._used = true;
    if (this._boundaryChecker.write(item)) {
      this.handleChunkBoundary();
    }
  }

  async skip(): Promise<void> {
    const cursor = notNull(this._cursor);

    if (await cursor.advance() && cursor.indexInChunk === 0) {
      await this.skipParentIfExists();
    }
  }

  async skipParentIfExists(): Promise<void> {
    if (this._parent && this._parent._cursor) {
      await this._parent.skip();
    }
  }

  createParent() {
    invariant(!this._parent);
    this._parent = new SequenceChunker(
        this._cursor && this._cursor.parent ? this._cursor.parent.clone() : null,
        this._parentMakeChunk,
        this._parentMakeChunk,
        this._newBoundaryChecker(),
        this._newBoundaryChecker);
  }

  handleChunkBoundary() {
    invariant(this._current.length > 0);
    if (!this._parent) {
      invariant(!this._isOnChunkBoundary);
      this._isOnChunkBoundary = true;
    } else {
      invariant(this._current.length > 0);
      const chunk = this._makeChunk(this._current)[0];
      notNull(this._parent).append(chunk);
      this._current = [];
    }
  }

  async done(): Promise<any> {
    if (this._cursor) {
      await this.finalizeCursor();
    }

    if (this.isRoot()) {
      return this._makeChunk(this._current)[1];
    }

    if (this._current.length > 0) {
      this.handleChunkBoundary();
    }

    invariant(this._parent);
    return this._parent.done();
  }

  isRoot(): boolean {
    for (let ancestor = this._parent; ancestor; ancestor = ancestor._parent) {
      if (ancestor._used) {
        return false;
      }
    }

    return true;
  }

  async finalizeCursor(): Promise<void> {
    const cursor = notNull(this._cursor);
    if (!cursor.valid) {
      await this.skipParentIfExists();
      return;
    }

    const fzr = cursor.clone();
    let i = 0;
    for (; i < this._boundaryChecker.windowSize || fzr.indexInChunk > 0; i++) {
      if (i === 0 || fzr.indexInChunk === 0) {
        await this.skipParentIfExists();
      }
      this.append(fzr.getCurrent());
      if (!await fzr.advance()) {
        break;
      }
    }
  }
}
