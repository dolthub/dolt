// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import Collection from './collection.js';
import {IndexedSequence} from './indexed-sequence.js';
import {SequenceCursor} from './sequence.js';
import {invariant} from './assert.js';
import type {ValueReader, ValueWriter, ValueReadWriter} from './value-store.js';
import {blobType} from './type.js';
import {
  OrderedKey,
  MetaTuple,
  newIndexedMetaSequenceChunkFn,
  newIndexedMetaSequenceBoundaryChecker,
} from './meta-sequence.js';
import BuzHashBoundaryChecker from './buzhash-boundary-checker.js';
import Ref from './ref.js';
import SequenceChunker from './sequence-chunker.js';
import type {BoundaryChecker, makeChunkFn} from './sequence-chunker.js';
import {Kind} from './noms-kind.js';
import type {EqualsFn} from './edit-distance.js';
import Bytes from './bytes.js';

export default class Blob extends Collection<IndexedSequence> {
  constructor(bytes: Uint8Array) {
    const w = new BlobWriter();
    w.write(bytes);
    w.close();
    super(w.blob.sequence);
  }

  getReader(): BlobReader {
    return new BlobReader(this.sequence);
  }

  get length(): number {
    return this.sequence.numLeaves;
  }
}

export class BlobReader {
  _sequence: IndexedSequence;
  _cursor: Promise<SequenceCursor<number, IndexedSequence<number>>>;
  _pos: number;
  _lock: string;

  constructor(sequence: IndexedSequence) {
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

  _readCur(cur: SequenceCursor): Promise<Uint8Array> {
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

  getOffset(idx: number): number {
    return idx;
  }

  getCompareFn(other: IndexedSequence): EqualsFn {
    return (idx: number, otherIdx: number) =>
      this.items[idx] === other.items[otherIdx];
  }
}

const blobWindowSize = 64;
const blobPattern = ((1 << 11) | 0) - 1; // Avg Chunk Size: 2k

function newBlobLeafChunkFn(vr: ?ValueReader, vw: ?ValueWriter): makeChunkFn {
  return (items: Array<number>) => {
    const blobLeaf = new BlobLeafSequence(vr, Bytes.fromValues(items));
    const blob = Blob.fromSequence(blobLeaf);
    const key = new OrderedKey(items.length);
    let mt;
    if (vw) {
      mt = new MetaTuple(vw.writeValue(blob), key, items.length, null);
    } else {
      mt = new MetaTuple(new Ref(blob), key, items.length, blob);
    }
    return [mt, blobLeaf];
  };
}

function newBlobLeafBoundaryChecker(): BoundaryChecker<number> {
  return new BuzHashBoundaryChecker(blobWindowSize, 1, blobPattern, (v: number) => v);
}

type BlobWriterState = 'writable' | 'closed';

export class BlobWriter {
  _state: BlobWriterState;
  _blob: ?Blob;
  _chunker: SequenceChunker;

  constructor(vrw: ?ValueReadWriter) {
    this._state = 'writable';
    this._chunker = new SequenceChunker(null, newBlobLeafChunkFn(vrw, vrw),
        newIndexedMetaSequenceChunkFn(Kind.Blob, vrw, vrw), newBlobLeafBoundaryChecker(),
        newIndexedMetaSequenceBoundaryChecker);
  }

  write(chunk: Uint8Array) {
    assert(this._state === 'writable');
    for (let i = 0; i < chunk.length; i++) {
      this._chunker.append(chunk[i]);
    }
  }

  close() {
    assert(this._state === 'writable');
    this._blob = Blob.fromSequence(this._chunker.doneSync());
    this._state = 'closed';
  }

  get blob(): Blob {
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
