// @flow

import {Collection} from './collection.js';
import {IndexedSequence} from './indexed-sequence.js';
import {SequenceCursor} from './sequence.js';
import {invariant} from './assert.js';
import type DataStore from './data-store.js';
import {blobType} from './type.js';
import {
  MetaTuple,
  newIndexedMetaSequenceChunkFn,
  newIndexedMetaSequenceBoundaryChecker,
} from './meta-sequence.js';
import BuzHashBoundaryChecker from './buzhash-boundary-checker.js';
import {SequenceChunker} from './sequence-chunker.js';
import type {BoundaryChecker, makeChunkFn} from './sequence-chunker.js';
import type {uint8} from './primitives.js';

export class NomsBlob extends Collection<IndexedSequence<uint8>> {
  constructor(sequence: IndexedSequence<uint8>) {
    super(blobType, sequence);
  }

  getReader(): BlobReader {
    return new BlobReader(this.sequence.newCursorAt(0));
  }

  get length(): number {
    return this.sequence.numLeaves;
  }
}

export class BlobReader {
  _cursor: Promise<SequenceCursor<number, IndexedSequence<number>>>;
  _lock: boolean;

  constructor(cursor: Promise<SequenceCursor<number, IndexedSequence<number>>>) {
    this._cursor = cursor;
    this._lock = false;
  }

  async read(): Promise<{done: boolean, value?: Uint8Array}> {
    invariant(!this._lock, 'cannot read without completing current read');
    this._lock = true;

    const cur = await this._cursor;
    if (!cur.valid) {
      return {done: true};
    }

    const arr = cur.sequence.items;
    await cur.advanceChunk();

    // No more awaits after this, so we can't be interrupted.
    this._lock = false;

    invariant(arr instanceof Uint8Array);
    return {done: false, value: arr};
  }
}

export class BlobLeafSequence extends IndexedSequence<uint8> {
  constructor(cs: ?DataStore, items: Uint8Array) {
    // $FlowIssue: The super class expects Array<T> but we sidestep that.
    super(cs, blobType, items);
  }

  getOffset(idx: number): number {
    return idx;
  }
}

const blobWindowSize = 64;
const blobPattern = ((1 << 13) | 0) - 1;

function newBlobLeafChunkFn(cs: ?DataStore = null): makeChunkFn {
  return (items: Array<uint8>) => {
    const blobLeaf = new BlobLeafSequence(cs, new Uint8Array(items));
    const mt = new MetaTuple(blobLeaf, items.length, items.length);
    return [mt, blobLeaf];
  };
}

function newBlobLeafBoundaryChecker(): BoundaryChecker<uint8> {
  return new BuzHashBoundaryChecker(blobWindowSize, 1, blobPattern, (v: uint8) => v);
}

export function newBlob(bytes: Uint8Array): Promise<NomsBlob> {
  const w = new BlobWriter();
  w.write(bytes);
  return w.close().then(() => w.blob);
}

type BlobWriterState = 'writable' | 'closing' | 'closed';

export class BlobWriter {
  _state: BlobWriterState;
  _blob: ?NomsBlob;
  _chunker: SequenceChunker;

  constructor() {
    this._state = 'writable';
    this._chunker = new SequenceChunker(null, newBlobLeafChunkFn(),
        newIndexedMetaSequenceChunkFn(blobType), newBlobLeafBoundaryChecker(),
        newIndexedMetaSequenceBoundaryChecker);
  }

  write(chunk: Uint8Array) {
    assert(this._state === 'writable');
    for (let i = 0; i < chunk.length; i++) {
      this._chunker.append(chunk[i]);
    }
  }

  async close(): Promise<void> {
    assert(this._state === 'writable');
    this._state = 'closing';
    const sequence = await this._chunker.done();
    this._blob = new NomsBlob(sequence);
    this._state = 'closed';
  }

  get blob(): NomsBlob {
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
