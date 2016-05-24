// @flow

import type Sequence from './sequence.js'; // eslint-disable-line no-unused-vars
import {invariant, notNull} from './assert.js';
import type {Collection} from './collection.js';
import type {MetaSequence, MetaTuple} from './meta-sequence.js';
import type {SequenceCursor} from './sequence.js';

export type BoundaryChecker<T> = {
  write: (item: T) => boolean;
  windowSize: number;
}

export type NewBoundaryCheckerFn = () => BoundaryChecker<MetaTuple>;

export type makeChunkFn<T: Collection> = (items: Array<any>) => [MetaTuple, T];

export async function chunkSequence<C: Collection, S>(
    cursor: SequenceCursor,
    insert: Array<S>,
    remove: number,
    makeChunk: makeChunkFn<C>,
    parentMakeChunk: makeChunkFn<C>,
    boundaryChecker: BoundaryChecker<S>,
    newBoundaryChecker: NewBoundaryCheckerFn): Promise<C> {

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

  insert.forEach(i => chunker.append(i));

  return chunker.done();
}

// Like |chunkSequence|, but without an existing cursor (implying this is a new collection), so it
// can be synchronous. Necessary for constructing collections without a Promises or async/await.
// There is no equivalent in the Go code because Go is already synchronous.
export function chunkSequenceSync<C: Collection, S>(
    insert: Array<S>,
    makeChunk: makeChunkFn<C>,
    parentMakeChunk: makeChunkFn<C>,
    boundaryChecker: BoundaryChecker<S>,
    newBoundaryChecker: NewBoundaryCheckerFn): C {

  const chunker = new SequenceChunker(null, makeChunk, parentMakeChunk, boundaryChecker,
                                      newBoundaryChecker);

  insert.forEach(i => chunker.append(i));

  return chunker.doneSync();
}

export default class SequenceChunker<C: Collection, S, U:Sequence> {
  _cursor: ?SequenceCursor<S, U>;
  _isOnChunkBoundary: boolean;
  _parent: ?SequenceChunker<C, MetaTuple, MetaSequence>;
  _current: Array<S>;
  _makeChunk: makeChunkFn<C>;
  _parentMakeChunk: makeChunkFn<C>;
  _boundaryChecker: BoundaryChecker<S>;
  _newBoundaryChecker: NewBoundaryCheckerFn;
  _used: boolean;

  constructor(cursor: ?SequenceCursor, makeChunk: makeChunkFn,
              parentMakeChunk: makeChunkFn,
              boundaryChecker: BoundaryChecker<S>,
              newBoundaryChecker: NewBoundaryCheckerFn) {
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
    const parent = this._parent;
    if (!parent) {
      invariant(!this._isOnChunkBoundary);
      this._isOnChunkBoundary = true;
    } else {
      invariant(this._current.length > 0);
      const chunk = this._makeChunk(this._current)[0];
      parent.append(chunk);
      this._current = [];
    }
  }

  async done(): Promise<C> {
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

  // Like |done|, but assumes there is no cursor, so it can be synchronous. Necessary for
  // constructing collections without Promises or async/await. There is no equivalent in the Go
  // code because Go is already synchronous.
  doneSync(): C {
    invariant(!this._cursor);

    if (this.isRoot()) {
      return this._makeChunk(this._current)[1];
    }

    if (this._current.length > 0) {
      this.handleChunkBoundary();
    }

    invariant(this._parent);
    return this._parent.doneSync();
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
