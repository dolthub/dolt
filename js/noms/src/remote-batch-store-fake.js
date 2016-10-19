// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import Chunk from './chunk.js';
import Hash from './hash.js';
import MemoryStore from './memory-store.js';
import RemoteBatchStore from './remote-batch-store.js';
import type {BatchStore} from './batch-store.js';
import type {ChunkStore} from './chunk-store.js';
import type {ChunkStream} from './chunk-serializer.js';
import type {UnsentReadMap} from './remote-batch-store.js';
import {notNull} from './assert.js';

export default function makeRemoteBatchStoreFake(): BatchStore {
  return new RemoteBatchStore(3, new TestingDelegate(new MemoryStore()));
}

export class TestingDelegate {
  _cs: ChunkStore;
  preUpdateRootHook: () => Promise<void>;

  constructor(cs: ChunkStore) {
    this._cs = cs;
    this.preUpdateRootHook = () => Promise.resolve();
  }

  async readBatch(reqs: UnsentReadMap): Promise<void> {
    Object.keys(reqs).forEach(hashStr => {
      this._cs.get(notNull(Hash.parse(hashStr))).then(chunk => { reqs[hashStr](chunk); });
    });
  }

  async writeBatch(hints: Set<Hash>, chunkStream: ChunkStream): Promise<void> {
    return chunkStream((chunk: Chunk) => this._cs.put(chunk));
  }

  async getRoot(): Promise<Hash> {
    return this._cs.getRoot();
  }

  async updateRoot(current: Hash, last: Hash): Promise<boolean> {
    return this.preUpdateRootHook().then(() => this._cs.updateRoot(current, last));
  }
}
