// @flow

import {Collection} from './collection.js';
import {IndexedSequence} from './indexed_sequence.js';
import {SequenceCursor} from './sequence.js';
import {invariant} from './assert.js';
import type {ChunkStore} from './chunk_store.js';
import {blobType} from './type.js';
import type {uint8} from './primitives.js';

export class NomsBlob extends Collection<IndexedSequence<uint8>> {
  constructor(sequence: IndexedSequence<uint8>) {
    super(blobType, sequence);
  }
  getReader(): BlobReader {
    return new BlobReader(this.sequence.newCursorAt(0));
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
  constructor(cs: ChunkStore, items: Uint8Array) {
    // $FlowIssue: The super class expects Array<T> but we sidestep that.
    super(cs, blobType, items);
  }

  getOffset(idx: number): number {
    return idx;
  }
}

export function newBlob(data: Uint8Array, cs: ChunkStore): NomsBlob {
  // TODO: Chunk it!
  return new NomsBlob(new BlobLeafSequence(cs, data));
}
