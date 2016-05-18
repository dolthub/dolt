// @flow

import type Chunk from '../chunk.js';

type ChunkStream = (cb: (chunk: Chunk) => void) => Promise<void>

const err = () => new Error('not implemented');
const rej = () => Promise.reject(err());

export default class OrderedPutCache {
  append(): boolean {
    throw err();
  }

  get(): ?Promise<Chunk> {
    throw err();
  }

  dropUntil(): Promise<void> {
    return rej();
  }

  extractChunks(): Promise<ChunkStream> {
    return rej();
  }

  destroy(): Promise<void> {
    return rej();
  }
}
