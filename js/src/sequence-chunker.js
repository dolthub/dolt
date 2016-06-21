// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import type Sequence from './sequence.js'; // eslint-disable-line no-unused-vars
import {invariant, notNull} from './assert.js';
import type {MetaSequence, MetaTuple} from './meta-sequence.js';
import type {SequenceCursor} from './sequence.js';

export type BoundaryChecker<T> = {
  write: (item: T) => boolean;
  windowSize: number;
};

export type NewBoundaryCheckerFn = () => BoundaryChecker<MetaTuple>;

export type makeChunkFn<T, S: Sequence> = (items: Array<T>) => [MetaTuple, S];

export async function chunkSequence<T, S: Sequence<T>>(
    cursor: SequenceCursor,
    insert: Array<T>,
    remove: number,
    makeChunk: makeChunkFn<T, S>,
    parentMakeChunk: makeChunkFn<MetaTuple, MetaSequence>,
    boundaryChecker: BoundaryChecker<T>,
    newBoundaryChecker: NewBoundaryCheckerFn): Promise<Sequence> {

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
export function chunkSequenceSync<T, S: Sequence<T>>(
    insert: Array<T>,
    makeChunk: makeChunkFn<T, S>,
    parentMakeChunk: makeChunkFn<MetaTuple, MetaSequence>,
    boundaryChecker: BoundaryChecker<T>,
    newBoundaryChecker: NewBoundaryCheckerFn): Sequence {

  const chunker = new SequenceChunker(null, makeChunk, parentMakeChunk, boundaryChecker,
                                      newBoundaryChecker);

  insert.forEach(i => chunker.append(i));

  return chunker.doneSync();
}

export default class SequenceChunker<T, S: Sequence<T>> {
  _cursor: ?SequenceCursor<T, S>;
  _parent: ?SequenceChunker<MetaTuple, MetaSequence>;
  _current: Array<T>;
  _lastSeq: ?S;
  _makeChunk: makeChunkFn<T, S>;
  _parentMakeChunk: makeChunkFn<MetaTuple, MetaSequence>;
  _boundaryChecker: BoundaryChecker<T>;
  _newBoundaryChecker: NewBoundaryCheckerFn;
  _done: boolean;

  constructor(cursor: ?SequenceCursor, makeChunk: makeChunkFn,
              parentMakeChunk: makeChunkFn,
              boundaryChecker: BoundaryChecker<T>,
              newBoundaryChecker: NewBoundaryCheckerFn) {
    this._cursor = cursor;
    this._parent = null;
    this._current = [];
    this._lastSeq = null;
    this._makeChunk = makeChunk;
    this._parentMakeChunk = parentMakeChunk;
    this._boundaryChecker = boundaryChecker;
    this._newBoundaryChecker = newBoundaryChecker;
    this._done = false;
  }

  async resume(): Promise<void> {
    const cursor = notNull(this._cursor);
    if (cursor.parent) {
      this.createParent();
      await notNull(this._parent).resume();
    }

    // Number of previous items which must be hashed into the boundary checker.
    let primeHashCount = this._boundaryChecker.windowSize - 1;

    // If the cursor is beyond the final position in the sequence, the preceeding
    // item may have been a chunk boundary. In that case, we must test at least the preceeding item.
    const appendPenultimate = cursor.idx === cursor.length;
    if (appendPenultimate) {
      // In that case, we prime enough items *prior* to the penultimate item to be correct.
      primeHashCount++;
    }

    // Number of items preceeding initial cursor in present chunk.
    const primeCurrentCount = cursor.indexInChunk;

    // Number of items to fetch prior to cursor position
    const prevCount = Math.max(primeHashCount, primeCurrentCount);

    const prev = await cursor.maxNPrevItems(prevCount);
    for (let i = 0; i < prev.length; i++) {
      const item = prev[i];
      const backIdx = prev.length - i;
      if (appendPenultimate && backIdx === 1) {
        // Test the penultimate item for a boundary.
        this.append(item);
        continue;
      }

      if (backIdx <= primeHashCount) {
        this._boundaryChecker.write(item);
      }
      if (backIdx <= primeCurrentCount) {
        this._current.push(item);
      }
    }
  }

  append(item: T) {
    this._current.push(item);
    if (this._boundaryChecker.write(item)) {
      this.handleChunkBoundary(true);
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

  handleChunkBoundary(createParentIfNil: boolean) {
    invariant(this._current.length > 0);
    const [chunk, seq] = this._makeChunk(this._current);
    this._current = [];
    this._lastSeq = seq;
    if (!this._parent && createParentIfNil) {
      this.createParent();
    }
    if (this._parent) {
      this._parent.append(chunk);
    }
  }

  async done(): Promise<Sequence> {
    invariant(!this._done);
    this._done = true;

    for (let s = this; s; s = s._parent) {
      if (s._cursor) {
        await s.finalizeCursor();
      }
    }

    // Chunkers will probably have current items which didn't hit a chunk boundary. Pretend they end
    // on chunk boundaries for now.
    this.finalizeChunkBoundaries();

    // The rest of this code figures out which sequence in the parent chain is canonical. That is:
    // * It's empty, or
    // * It never chunked, so it's not a prollytree, or
    // * It chunked, so it's a prollytree, but it must have at least 2 children (or it could have
    //   been represented as that 1 child).
    //
    // Examples of when we may have constructed non-canonical sequences:
    // * If the previous tree (i.e. its cursor) was deeper, we will have created empty parents.
    // * If the last appended item was on a chunk boundary, there may be a sequence with a single
    //   chunk.

    // Firstly, follow up the parent chain to find the highest chunker which did chunk.
    let seq = this.findRoot();
    if (!seq) {
      seq = this._makeChunk([])[1];
      return seq;
    }

    // Lastly, step back down to find a meta sequence with more than 1 child.
    while (seq.length <= 1) {
      invariant(seq.length !== 0);
      if (!seq.isMeta) {
        break;
      }
      seq = notNull(await seq.getChildSequence(0));
    }

    return notNull(seq); // flow should not need this notNull
  }

  // Like |done|, but assumes there is no cursor, so it can be synchronous. Necessary for
  // constructing collections without Promises or async/await. There is no equivalent in the Go
  // code because Go is already synchronous.
  doneSync(): Sequence {
    invariant(!this._cursor);
    invariant(!this._done);
    this._done = true;

    this.finalizeChunkBoundaries();

    let seq = this.findRoot();
    if (!seq) {
      seq = this._makeChunk([])[1];
      return seq;
    }

    while (seq.length <= 1) {
      invariant(seq.length !== 0);
      if (!seq.isMeta) {
        break;
      }
      seq = notNull(seq.getChildSequenceSync(0));
    }

    return notNull(seq); // flow should not need this notNull
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

  finalizeChunkBoundaries() {
    for (let s = this; s; s = s._parent) {
      if (s._current.length > 0) {
        // Don't create a new parent if we haven't chunked.
        s.handleChunkBoundary(Boolean(s._lastSeq));
      }
    }
  }

  findRoot(): ?Sequence {
    let root = null;
    for (let s = this; s; s = s._parent) {
      if (s._lastSeq) {
        root = s._lastSeq;
      }
    }
    return root;
  }
}
