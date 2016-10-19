// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import Chunk from './chunk.js';
import Hash from './hash.js';
import type {ChunkStore} from './chunk-store.js';

export interface BatchStore {
  get(hash: Hash): Promise<Chunk>;
  schedulePut(c: Chunk, hints: Set<Hash>): void;
  flush(): Promise<void>;
  getRoot(): Promise<Hash>;
  updateRoot(current: Hash, last: Hash): Promise<boolean>;
  close(): Promise<void>;
}

export class BatchStoreAdaptor {
  _cs: ChunkStore;

  constructor(cs: ChunkStore) {
    this._cs = cs;
  }

  get(hash: Hash): Promise<Chunk> {
    return this._cs.get(hash);
  }

  schedulePut(c: Chunk, hints: Set<Hash>): void { // eslint-disable-line no-unused-vars
    this._cs.put(c);
  }

  flush(): Promise<void> {
    return Promise.resolve();
  }

  getRoot(): Promise<Hash> {
    return this._cs.getRoot();
  }

  updateRoot(current: Hash, last: Hash): Promise<boolean> {
    return this._cs.updateRoot(current, last);
  }

  close(): Promise<void> {
    return Promise.resolve();
  }
}
