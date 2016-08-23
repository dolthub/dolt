// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import * as Bytes from './bytes.js';
import Collection from './collection.js';
import RollingValueHasher from './rolling-value-hasher.js';
import {default as SequenceChunker, chunkSequence} from './sequence-chunker.js';
import type {EqualsFn} from './edit-distance.js';
import type {ValueReader, ValueReadWriter} from './value-store.js';
import type {makeChunkFn} from './sequence-chunker.js';
import {IndexedSequence} from './indexed-sequence.js';
import {Kind} from './noms-kind.js';
import {OrderedKey, newIndexedMetaSequenceChunkFn} from './meta-sequence.js';
import {SequenceCursor} from './sequence.js';
import {blobType} from './type.js';
import {invariant} from './assert.js';
import {hashValueByte} from './rolling-value-hasher.js';

export default class Blob extends Collection<IndexedSequence<any>> {
  constructor(bytes: Uint8Array) {
    const chunker = new SequenceChunker(null, null, null, newBlobLeafChunkFn(null),
        newIndexedMetaSequenceChunkFn(Kind.Blob, null), blobHashValueBytes);

    for (let i = 0; i < bytes.length; i++) {
      chunker.append(bytes[i]);
    }

    const seq = chunker.doneSync();
    invariant(seq instanceof IndexedSequence);
    super(seq);
  }

  getReader(): BlobReader {
    return new BlobReader(this.sequence);
  }

  get length(): number {
    return this.sequence.numLeaves;
  }

  splice(idx: number, deleteCount: number, insert: Uint8Array): Promise<Blob> {
    const vr = this.sequence.vr;
    return this.sequence.newCursorAt(idx).then(cursor =>
      chunkSequence(cursor, vr, Array.from(insert), deleteCount, newBlobLeafChunkFn(vr),
                    newIndexedMetaSequenceChunkFn(Kind.Blob, vr, null),
                    hashValueByte)).then(s => Blob.fromSequence(s));
  }
}

export class BlobReader {
  _sequence: IndexedSequence<any>;
  _cursor: Promise<SequenceCursor<number, IndexedSequence<number>>>;
  _pos: number;
  _lock: string;

  constructor(sequence: IndexedSequence<any>) {
    this._sequence = sequence;
    this._cursor = sequence.newCursorAt(0);
    this._pos = 0;
    this._lock = '';
  }

  /**
   * Reads the next chunk of bytes from this blob.
   *
   * Returns {done: false, value: chunk} if there is more data, or {done: true} if there is none.
   */
  read(): Promise<{done: boolean, value?: Uint8Array}> {
    invariant(this._lock === '', `cannot read without completing current ${this._lock}`);
    this._lock = 'read';

    return this._cursor.then(cur => {
      if (!cur.valid) {
        return {done: true};
      }
      return this._readCur(cur).then(arr => ({done: false, value: arr}));
    }).then(res => {
      this._lock = '';
      return res;
    });
  }

  _readCur(cur: SequenceCursor<any, any>): Promise<Uint8Array> {
    let arr = cur.sequence.items;
    invariant(arr instanceof Uint8Array);

    const idx = cur.indexInChunk;
    if (idx > 0) {
      invariant(idx < arr.byteLength);
      arr = Bytes.subarray(arr, idx, arr.byteLength);
    }

    return cur.advanceChunk().then(() => {
      this._pos += arr.byteLength;
      return arr;
    });
  }

  /**
   * Seeks the reader to a position either relative to the start, the current position, or end of
   * the blob.
   *
   * If |whence| is 0, |offset| will be relative to the start.
   * If |whence| is 1, |offset| will be relative to the current position.
   * If |whence| is 2, |offset| will be relative to the end.
   */
  seek(offset: number, whence: number = 0): Promise<number> {
    invariant(this._lock === '', `cannot seek without completing current ${this._lock}`);
    this._lock = 'seek';

    let abs = this._pos;

    switch (whence) {
      case 0:
        abs = offset;
        break;
      case 1:
        abs += offset;
        break;
      case 2:
        abs = this._sequence.numLeaves + offset;
        break;
      default:
        throw new Error(`invalid whence ${whence}`);
    }

    invariant(abs >= 0, `cannot seek to negative position ${abs}`);

    this._cursor = this._sequence.newCursorAt(abs);

    // Wait for the seek to complete so that reads will be relative to the new position.
    return this._cursor.then(() => {
      this._pos = abs;
      this._lock = '';
      return abs;
    });
  }
}

export class BlobLeafSequence extends IndexedSequence<number> {
  constructor(vr: ?ValueReader, items: Uint8Array) {
    // $FlowIssue: The super class expects Array<T> but we sidestep that.
    super(vr, blobType, items);
  }

  cumulativeNumberOfLeaves(idx: number): number {
    return idx + 1;
  }

  getCompareFn(other: IndexedSequence<any>): EqualsFn {
    return (idx: number, otherIdx: number) =>
      this.items[idx] === other.items[otherIdx];
  }
}

function newBlobLeafChunkFn(vr: ?ValueReader): makeChunkFn<any, any> {
  return (items: Array<number>) => {
    const blobLeaf = new BlobLeafSequence(vr, Bytes.fromValues(items));
    const blob = Blob.fromSequence(blobLeaf);
    const key = new OrderedKey(items.length);
    return [blob, key, items.length];
  };
}

function blobHashValueBytes(b: number, rv: RollingValueHasher) {
  rv.hashByte(b);
}

type BlobWriterState = 'writable' | 'closed';

export class BlobWriter {
  _state: BlobWriterState;
  _blob: ?Promise<Blob>;
  _chunker: SequenceChunker<any, any>;
  _vrw: ?ValueReadWriter;

  constructor(vrw: ?ValueReadWriter) {
    this._state = 'writable';
    this._chunker = new SequenceChunker(null, vrw, vrw, newBlobLeafChunkFn(vrw),
        newIndexedMetaSequenceChunkFn(Kind.Blob, vrw), blobHashValueBytes);
    this._vrw = vrw;
  }

  write(chunk: Uint8Array) {
    assert(this._state === 'writable');
    for (let i = 0; i < chunk.length; i++) {
      this._chunker.append(chunk[i]);
    }
  }

  close() {
    assert(this._state === 'writable');
    this._blob = this._chunker.done(this._vrw).then(seq => Blob.fromSequence(seq));
    this._state = 'closed';
  }

  get blob(): Promise<Blob> {
    assert(this._state === 'closed');
    invariant(this._blob);
    return this._blob;
  }
}

function assert(v: any) {
  if (!v) {
    throw new TypeError('Invalid usage of BlobWriter');
  }
}
