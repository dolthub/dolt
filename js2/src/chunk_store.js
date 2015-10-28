/* @flow */

'use strict';

const Chunk = require('./chunk.js');
const Ref = require('./ref.js');

export type ChunkStore = {
  getRoot(): Promise<Ref>;
  updateRoot(current: Ref, last: Ref): Promise<boolean>;
  get(ref: Ref): Promise<Chunk>;
  has(ref: Ref): Promise<boolean>;
  put(c: Chunk): void;
}
