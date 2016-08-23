// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import Chunk from './chunk.js';
import Hash from './hash.js';
import MemoryStore from './memory-store.js';
import OrderedPutCache from './put-cache.js';
import type {BatchStore} from './batch-store.js';
import type {ChunkStore} from './chunk-store.js';
import type {ChunkStream} from './chunk-serializer.js';
import {notNull} from './assert.js';

type PendingReadMap = { [key: string]: Promise<Chunk> };
export type UnsentReadMap = { [key: string]: (c: Chunk) => void };

export type WriteRequest = {
  hash: Hash;
  hints: Set<Hash>;
}

interface Delegate {
  readBatch(reqs: UnsentReadMap): Promise<void>;
  writeBatch(hints: Set<Hash>, chunkStream: ChunkStream): Promise<void>;
  getRoot(): Promise<Hash>;
  updateRoot(current: Hash, last: Hash): Promise<boolean>;
}

export default class RemoteBatchStore {
  _pendingReads: PendingReadMap;
  _unsentReads: ?UnsentReadMap;
  _readScheduled: boolean;
  _activeReads: number;
  _maxReads: number;

  _pendingWrites: OrderedPutCache;
  _unsentWrites: ?Array<WriteRequest>;
  _delegate: Delegate;

  constructor(maxReads: number, delegate: Delegate) {
    this._pendingReads = Object.create(null);
    this._unsentReads = null;
    this._readScheduled = false;
    this._activeReads = 0;
    this._maxReads = maxReads;

    this._pendingWrites = new OrderedPutCache();
    this._unsentWrites = null;
    this._delegate = delegate;
  }

  get(hash: Hash): Promise<Chunk> {
    const hashStr = hash.toString();
    let p = this._pendingReads[hashStr];
    if (p) {
      return p;
    }
    p = this._pendingWrites.get(hashStr);
    if (p) {
      return p;
    }

    return this._pendingReads[hashStr] = new Promise(resolve => {
      if (!this._unsentReads) {
        this._unsentReads = Object.create(null);
      }

      notNull(this._unsentReads)[hashStr] = resolve;
      this._maybeStartRead();
    });
  }

  _maybeStartRead() {
    if (!this._readScheduled && this._unsentReads && this._activeReads < this._maxReads) {
      this._readScheduled = true;
      setTimeout(() => {
        this._read();
      }, 0);
    }
  }

  async _read(): Promise<void> {
    this._activeReads++;

    const reqs = notNull(this._unsentReads);
    this._unsentReads = null;
    this._readScheduled = false;

    await this._delegate.readBatch(reqs);

    const self = this; // TODO: Remove this when babel bug is fixed.
    Object.keys(reqs).forEach(hashStr => {
      delete self._pendingReads[hashStr];
    });

    this._activeReads--;
    this._maybeStartRead();
  }

  schedulePut(c: Chunk, hints: Set<Hash>): void {
    if (!this._pendingWrites.append(c)) {
      return; // Already in flight.
    }

    if (!this._unsentWrites) {
      this._unsentWrites = [];
    }
    this._unsentWrites.push({hash: c.hash, hints: hints});
  }

  async flush(): Promise<void> {
    if (!this._unsentWrites) {
      return;
    }

    const reqs = notNull(this._unsentWrites);
    this._unsentWrites = null;

    const first = reqs[0].hash;
    let last = first;
    const hints = new Set();
    for (const req of reqs) {
      req.hints.forEach(hint => hints.add(hint));
      last = req.hash;
    }
    // TODO: Deal with backpressure
    const chunkStream = await this._pendingWrites.extractChunks(first.toString(), last.toString());
    await this._delegate.writeBatch(hints, chunkStream);

    return this._pendingWrites.dropUntil(last.toString());
  }

  async getRoot(): Promise<Hash> {
    return this._delegate.getRoot();
  }

  async updateRoot(current: Hash, last: Hash): Promise<boolean> {
    await this.flush();
    if (current.equals(last)) {
      return true;
    }

    return this._delegate.updateRoot(current, last);
  }

  // TODO: Should close() call flush() and block until it's done? Maybe closing with outstanding
  // requests should be an error on both sides. TBD.
  close(): Promise<void> {
    return this._pendingWrites.destroy();
  }
}

export function makeTestingRemoteBatchStore(): BatchStore {
  return new RemoteBatchStore(3, new TestingDelegate(new MemoryStore()));
}

class TestingDelegate {
  _cs: ChunkStore;

  constructor(cs: ChunkStore) {
    this._cs = cs;
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
    return this._cs.updateRoot(current, last);
  }
}
