// @flow

import type Chunk from './chunk.js';
import type Hash from './hash.js';

export type ChunkStore = {
  getRoot(): Promise<Hash>;
  updateRoot(current: Hash, last: Hash): Promise<boolean>;
  get(hash: Hash): Promise<Chunk>;
  has(hash: Hash): Promise<boolean>;
  put(c: Chunk): void;
}

export interface RootTracker {
  getRoot(): Promise<Hash>;
  updateRoot(current: Hash, last: Hash): Promise<boolean>;
}
