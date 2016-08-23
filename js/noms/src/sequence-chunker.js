// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import type Sequence from './sequence.js'; // eslint-disable-line no-unused-vars
import type {MetaSequence, OrderedKey} from './meta-sequence.js';
import {metaHashValueBytes, MetaTuple} from './meta-sequence.js';
import type {SequenceCursor} from './sequence.js';
import type {ValueReader, ValueWriter} from './value-store.js';
import {invariant, notNull} from './assert.js';
import type Collection from './collection.js';
import RollingValueHasher from './rolling-value-hasher.js';

import Ref from './ref.js';

export type makeChunkFn<T, S: Sequence<any>> = (items: Array<T>) =>
    [Collection<S>, OrderedKey<any>, number];
export type hashValueBytesFn<T> = (item: T, rv: RollingValueHasher) => void;

export async function chunkSequence<T, S: Sequence<T>>(
    cursor: SequenceCursor<any, any>,
    vr: ?ValueReader,
    insert: Array<T>,
    remove: number,
    makeChunk: makeChunkFn<T, S>,
    parentMakeChunk: makeChunkFn<MetaTuple<any>, MetaSequence<any>>,
    hashValueBytes: hashValueBytesFn<any>): Promise<Sequence<any>> {

  const chunker = new SequenceChunker(cursor, vr, null, makeChunk, parentMakeChunk, hashValueBytes);
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
    parentMakeChunk: makeChunkFn<MetaTuple<any>, MetaSequence<any>>,
    hashValueBytes: hashValueBytesFn<any>): Sequence<any> {

  const chunker = new SequenceChunker(null, null, null, makeChunk, parentMakeChunk, hashValueBytes);

  insert.forEach(i => chunker.append(i));

  return chunker.doneSync();
}

export default class SequenceChunker<T, S: Sequence<T>> {
  _cursor: ?SequenceCursor<T, S>;
  _vr: ?ValueReader;
  _vw: ?ValueWriter;
  _parent: ?SequenceChunker<MetaTuple<any>, MetaSequence<any>>;
  _current: Array<T>;
  _makeChunk: makeChunkFn<T, S>;
  _parentMakeChunk: makeChunkFn<MetaTuple<any>, MetaSequence<any>>;
  _isLeaf: boolean;
  _hashValueBytes: hashValueBytesFn<any>;
  _rv: RollingValueHasher;
  _done: boolean;

  constructor(cursor: ?SequenceCursor<any, any>, vr: ?ValueReader, vw: ?ValueWriter,
              makeChunk: makeChunkFn<any, any>, parentMakeChunk: makeChunkFn<any, any>,
              hashValueBytes: hashValueBytesFn<any>) {
    this._cursor = cursor;
    this._vr = vr;
    this._vw = vw;
    this._parent = null;
    this._current = [];
    this._makeChunk = makeChunk;
    this._parentMakeChunk = parentMakeChunk;
    this._isLeaf = true;
    this._hashValueBytes = hashValueBytes;
    this._rv = new RollingValueHasher();
    this._done = false;
  }

  async resume(): Promise<void> {
    const cursor = notNull(this._cursor);
    if (cursor.parent) {
      this.createParent();
      await notNull(this._parent).resume();
    }

    // Number of previous items which must be hashed into the boundary checker.
    let primeHashBytes = this._rv.window;

    const retreater = cursor.clone();
    let appendCount = 0;
    let primeHashCount = 0;

    // If the cursor is beyond the final position in the sequence, then we can't tell the difference
    // between it having been an explicit and implicit boundary. Since the caller may be about to
    // append another value, we need to know whether the existing final item is an explicit chunk
    // boundary.
    const cursorBeyondFinal = cursor.idx === cursor.length;
    if (cursorBeyondFinal && await retreater._retreatMaybeAllowBeforeStart(false)) {
      // In that case, we prime enough items *prior* to the final item to be correct.
      appendCount++;
      primeHashCount++;
    }

    // Walk backwards to the start of the existing chunk.
    this._rv.lengthOnly = true;
    while (retreater.indexInChunk > 0 && await retreater._retreatMaybeAllowBeforeStart(false)) {
      appendCount++;
      if (primeHashBytes > 0) {
        primeHashCount++;
        this._rv.clearLastBoundary();
        this._hashValueBytes(retreater.getCurrent(), this._rv);
        primeHashBytes -= this._rv.bytesHashed;
      }
    }

    // If the hash window won't be filled by the preceeding items in the current chunk, walk
    // further back until they will.
    while (primeHashBytes > 0 && await retreater._retreatMaybeAllowBeforeStart(false)) {
      primeHashCount++;
      this._rv.clearLastBoundary();
      this._hashValueBytes(retreater.getCurrent(), this._rv);
      primeHashBytes -= this._rv.bytesHashed;
    }
    this._rv.lengthOnly = false;

    while (primeHashCount > 0 || appendCount > 0) {
      const item = retreater.getCurrent();
      await retreater.advance();

      if (primeHashCount > appendCount) {
        // Before the start of the current chunk: just hash value bytes into window.
        this._hashValueBytes(item, this._rv);
        primeHashCount--;
        continue;
      }

      if (appendCount > primeHashCount) {
        // In current chunk, but before window: just append item.
        this._current.push(item);
        appendCount--;
        continue;
      }

      this._rv.clearLastBoundary();
      this._hashValueBytes(item, this._rv);
      this._current.push(item);

      // Within current chunk and hash window: append item & hash value bytes into window.
      if (this._rv.crossedBoundary && cursorBeyondFinal && appendCount === 1) {
        // The cursor is positioned immediately after the final item in the sequence and it *was*
        // an *explicit* chunk boundary: create a chunk.
        this.handleChunkBoundary();
      }

      appendCount--;
      primeHashCount--;
    }
  }

  append(item: T) {
    this._current.push(item);
    this._rv.clearLastBoundary();
    this._hashValueBytes(item, this._rv);
    if (this._rv.crossedBoundary) {
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
        this._vr,
        this._vw,
        this._parentMakeChunk,
        this._parentMakeChunk,
        metaHashValueBytes);
    this._parent._isLeaf = false;
  }

  createSequence(): [Sequence<any>, MetaTuple<any>] {
    // If the sequence chunker has a ValueWriter, eagerly write sequences.
    let [col, key, numLeaves] = this._makeChunk(this._current); // eslint-disable-line prefer-const
    const seq = col.sequence;
    let ref: Ref<any>;
    if (this._vw) {
      ref = this._vw.writeValue(col);
      col = null;
    } else {
      ref = new Ref(col);
    }
    const mt = new MetaTuple(ref, key, numLeaves, col);
    this._current = [];
    return [seq, mt];
  }

  handleChunkBoundary() {
    invariant(this._current.length > 0);
    const mt = this.createSequence()[1];
    if (!this._parent) {
      this.createParent();
    }

    notNull(this._parent).append(mt);
  }

  // Returns true if this chunker or any of its parents have any pending items in their |current|
  // array.
  anyPending(): boolean {
    if (this._current.length > 0) {
      return true;
    }

    if (this._parent) {
      return this._parent.anyPending();
    }

    return false;
  }

  // Returns the root sequence of the resulting tree. The logic here is subtle, but hopefully
  // correct and understandable. See comments inline.
  async done(): Promise<Sequence<any>> {
    invariant(!this._done);
    this._done = true;

    if (this._cursor) {
      await this.finalizeCursor();
    }

    // There is pending content above us, so we must push any remaining items from this level up
    // and allow some parent to find the root of the resulting tree.
    if (this._parent && this._parent.anyPending()) {
      if (this._current.length > 0) {
        // If there are items in |current| at this point, they represent the final items of the
        // sequence which occurred beyond the previous *explicit* chunk boundary. The end of input
        // of a sequence is considered an *implicit* boundary.
        this.handleChunkBoundary();
      }

      return notNull(this._parent).done();
    }

    // At this point, we know this chunker contains, in |current| every item at this level of the
    // resulting tree. To see this, consider that there are two ways a chunker can enter items into
    // its |current|: (1) as the result of resume() with the cursor on anything other than the
    //  first item in the sequence, and (2) as a result of a child chunker hitting an explicit
    // chunk boundary during either Append() or finalize(). The only way there can be no items in
    // some parent chunker's |current| is if this chunker began with cursor within its first
    // existing chunk (and thus all parents resume()'d with a cursor on their first item) and
    // continued through all sebsequent items without creating any explicit chunk boundaries (and
    // thus never sent any items up to a parent as a result of chunking). Therefore, this chunker's
    // current must contain all items within the current sequence.

    // This level must represent *a* root of the tree, but it is possibly non-canonical. There are
    // three cases to consider:

    // (1) This is "leaf" chunker and thus produced tree of depth 1 which contains exactly one
    // chunk (never hit a boundary), or (2) This in an internal node of the tree which contains
    // multiple references to child nodes. In either case, this is the canonical root of the tree.
    if (this._isLeaf || this._current.length > 1) {
      return this.createSequence()[0];
    }

    // (3) This is an internal node of the tree which contains a single reference to a child node.
    // This can occur if a non-leaf chunker happens to chunk on the first item (metaTuple)
    // appended. In this case, this is the root of the tree, but it is *not* canonical and we must
    // walk down until we find cases (1) or (2), above.
    invariant(!this._isLeaf && this._current.length === 1);
    const mt = this._current[0];
    invariant(mt instanceof MetaTuple);
    let seq = await mt.getChildSequence(this._vr);

    while (seq.isMeta && seq.length === 1) {
      seq = await seq.getChildSequence(0);
    }

    return seq;
  }

  // Like |done|, but assumes there is no cursor, so it can be synchronous. Necessary for
  // constructing collections without Promises or async/await. There is no equivalent in the Go
  // code because Go is already synchronous.
  doneSync(): Sequence<any> {
    invariant(!this._vw);
    invariant(!this._cursor);
    invariant(!this._done);
    this._done = true;

    if (this._parent && this._parent.anyPending()) {
      if (this._current.length > 0) {
        this.handleChunkBoundary();
      }

      return notNull(this._parent).doneSync();
    }

    if (this._isLeaf || this._current.length > 1) {
      // Return the (possibly empty) sequence which never chunked.
      return this.createSequence()[0];
    }

    invariant(!this._isLeaf && this._current.length === 1);
    const mt = this._current[0];
    invariant(mt instanceof MetaTuple);
    let seq = mt.getChildSequenceSync();

    while (seq.isMeta && seq.length === 1) {
      seq = seq.getChildSequenceSync(0);
    }

    return seq;
  }

  // If we are mutating an existing sequence, appending subsequent items in the sequence until we
  // reach a pre-existing chunk boundary or the end of the sequence.
  async finalizeCursor(): Promise<void> {
    const cursor = notNull(this._cursor);
    if (!cursor.valid) {
      // The cursor is past the end, and due to the way cursors work, the parent cursor will
      // actually point to its last chunk. We need to force it to point past the end so that our
      // parent's Done() method doesn't add the last chunk twice.
      await this.skipParentIfExists();
      return;
    }

    // Append the rest of the values in the sequence, up to the window size, plus the rest of that
    // chunk. It needs to be the full window size because anything that was appended/skipped
    // between chunker construction and finalization will have changed the hash state.
    let hashWindow = this._rv.window;
    const fzr = cursor.clone();

    let isBoundary = this._current.length === 0;

    // We can terminate when: (1) we hit the end input in this sequence or (2) we process beyond
    // the hash window and encounter an item which is boundary in both the old and new state of the
    // sequence.
    let i = 0;
    for (; fzr.valid && (hashWindow > 0 || fzr.indexInChunk > 0 || !isBoundary); i++) {
      if (i === 0 || fzr.indexInChunk === 0) {
        // Every time we step into a chunk from the original sequence, that chunk will no longer
        // exist in the new sequence. The parent must be instructed to skip it.
        await this.skipParentIfExists();
      }

      const item = fzr.getCurrent();
      this._current.push(item);
      isBoundary = false;

      await fzr.advance();

      if (hashWindow > 0) {
        // While we are within the hash window, append items (which explicit checks the hash value
        // for chunk boundaries).
        this._rv.clearLastBoundary();
        this._hashValueBytes(item, this._rv);
        isBoundary = this._rv.crossedBoundary;
        hashWindow -= this._rv.bytesHashed;
      } else if (fzr.indexInChunk === 0) {
        // Once we are beyond the hash window, we know that boundaries can only occur in the same
        // place they did within the existing sequence.
        isBoundary = true;
      }

      if (isBoundary) {
        this.handleChunkBoundary();
      }
    }
  }
}
