/* @flow */

'use strict';

import Chunk from './chunk.js';
import Ref from './ref.js';

export type ChunkStore = {
  getRoot(): Promise<Ref>;
  updateRoot(current: Ref, last: Ref): Promise<boolean>;
  get(ref: Ref): Promise<Chunk>;
  has(ref: Ref): Promise<boolean>;
  put(c: Chunk): void;
}
