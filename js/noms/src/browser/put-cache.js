// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import type Chunk from '../chunk.js';
import {invariant} from '../assert.js';

type ChunkStream = (cb: (chunk: Chunk) => void) => Promise<void>
type ChunkIndex = Map<string, number>;

/**
 * Caches puts and allows enumeration of chunks in insertion order.
 */
export default class OrderedPutCache {
  _chunkIndex: ChunkIndex;
  _chunks: Chunk[];

  constructor() {
    init(this);
  }

  /**
   * Appends a chunk to the cache. If the chunk is already in the cache this returns false and
   * nothing is done to the cache.
   */
  append(c: Chunk): boolean {
    const hash = c.hash.toString();
    if (this._chunkIndex.has(hash)) {
      return false;
    }

    this._chunkIndex.set(hash, this._chunks.length);
    this._chunks.push(c);
    return true;
  }

  /**
   * Gets a chunk based on the hash.
   * This returns an empty chunk if the cache does not contain the given hash.
   */
  get(hash: string): Promise<Chunk> | null{
    if (!this._chunkIndex.has(hash)) {
      // TODO: This should be resolve(emptyChunk)
      return null;
    }

    const idx = this._chunkIndex.get(hash);
    invariant(typeof idx === 'number');
    return Promise.resolve(this._chunks[idx]);
  }

  /**
   * Removes the leading chunks from the cache up until (and including) the chunk with the hash
   * `limit`.
   */
  dropUntil(limit: string): Promise<void> {
    if (!this._chunkIndex.has(limit)) {
      return rej('Tried to drop unknown chunk: ' + limit);
    }

    const idx = this._chunkIndex.get(limit);
    invariant(idx !== undefined);
    for (const [k, v] of this._chunkIndex) {
      if (v <= idx) {
        this._chunkIndex.delete(k);
      } else {
        this._chunkIndex.set(k, v - idx - 1);
      }
    }
    this._chunks = this._chunks.slice(idx + 1);
    return Promise.resolve();
  }

  /**
   * Returns a stream that iterates over the chunks between `first` and `last` (inclusive).
   */
  extractChunks(first: string, last: string): Promise<ChunkStream> {
    const firstIndex = this._chunkIndex.get(first);
    const lastIndex = this._chunkIndex.get(last);
    if (firstIndex === undefined) {
      throw new Error('Tried to range from unknown chunk: ' + first);
    }
    if (lastIndex === undefined) {
      throw new Error('Tried to range to unknown chunk: ' + last);
    }

    const chunks = this._chunks.slice();  // Copy
    return Promise.resolve(cb => {
      for (let i = firstIndex; i <= lastIndex; i++) {
        cb(chunks[i]);
      }
      return Promise.resolve();
    });
  }

  /**
   * Removes the underlying backing store.
   */
  destroy(): Promise<void> {
    init(this);
    return Promise.resolve();
  }
}

function init(self: OrderedPutCache) {
  self._chunkIndex = new Map();
  self._chunks = [];
}

function rej(s: string): Promise<void> {
  return Promise.reject(new Error(s));
}
