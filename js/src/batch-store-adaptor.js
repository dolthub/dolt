// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import Chunk from './chunk.js';
import Hash from './hash.js';
import MemoryStore from './memory-store.js';
import BatchStore from './batch-store.js';
import type {ChunkStore} from './chunk-store.js';
import type {UnsentReadMap} from './batch-store.js';
import type {ChunkStream} from './chunk-serializer.js';

export function makeTestingBatchStore(): BatchStore {
  return new BatchStore(3, new BatchStoreAdaptorDelegate(new MemoryStore()));
}

export default class BatchStoreAdaptor extends BatchStore {
  constructor(cs: ChunkStore, maxReads: number = 3) {
    super(maxReads, new BatchStoreAdaptorDelegate(cs));
  }
}

export class BatchStoreAdaptorDelegate {
  _cs: ChunkStore;

  constructor(cs: ChunkStore) {
    this._cs = cs;
  }

  async readBatch(reqs: UnsentReadMap): Promise<void> {
    Object.keys(reqs).forEach(hashStr => {
      this._cs.get(Hash.parse(hashStr)).then(chunk => { reqs[hashStr](chunk); });
    });
  }

  async writeBatch(hints: Set<Hash>, chunkStream: ChunkStream): Promise<void> {
    return chunkStream((chunk: Chunk) => this._cs.put(chunk));
  }

  async getRoot(): Promise<Hash> {
    return this._cs.getRoot();
  }

  async updateRoot(current: Hash, last: Hash): Promise<boolean> {
    return this._cs.updateRoot(current, last);
  }
}
