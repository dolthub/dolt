// @flow

import Chunk from './chunk.js';
import Ref from './ref.js';
import {notNull} from './assert.js';

type PendingReadMap = { [key: string]: Promise<Chunk> };
export type UnsentReadMap = { [key: string]: (c: Chunk) => void };

type WriteMap = { [key: string]: Chunk };
export type WriteRequest = {
  c: Chunk;
  hints: Set<Ref>;
}

interface Delegate {
  readBatch(reqs: UnsentReadMap): Promise<void>;
  writeBatch(reqs: Array<WriteRequest>): Promise<void>;
  getRoot(): Promise<Ref>;
  updateRoot(current: Ref, last: Ref): Promise<boolean>;
}

export default class BatchStore {
  _pendingReads: PendingReadMap;
  _unsentReads: ?UnsentReadMap;
  _readScheduled: boolean;
  _activeReads: number;
  _maxReads: number;

  _pendingWrites: WriteMap;
  _unsentWrites: ?Array<WriteRequest>;
  _delegate: Delegate;

  constructor(maxReads: number, delegate: Delegate) {
    this._pendingReads = Object.create(null);
    this._unsentReads = null;
    this._readScheduled = false;
    this._activeReads = 0;
    this._maxReads = maxReads;

    this._pendingWrites = Object.create(null);
    this._unsentWrites = null;
    this._delegate = delegate;
  }

  get(ref: Ref): Promise<Chunk> {
    const refStr = ref.toString();
    let p = this._pendingReads[refStr];
    if (p) {
      return p;
    }
    p = this._pendingWrites[refStr];
    if (p) {
      return Promise.resolve(p);
    }

    return this._pendingReads[refStr] = new Promise(resolve => {
      if (!this._unsentReads) {
        this._unsentReads = Object.create(null);
      }

      notNull(this._unsentReads)[refStr] = resolve;
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
    Object.keys(reqs).forEach(refStr => {
      delete self._pendingReads[refStr];
    });

    this._activeReads--;
    this._maybeStartRead();
  }

  schedulePut(c: Chunk, hints: Set<Ref>): void {
    const refStr = c.ref.toString();
    if (this._pendingWrites[refStr]) {
      return; // Already in flight.
    }
    this._pendingWrites[refStr] = c;

    if (!this._unsentWrites) {
      this._unsentWrites = [];
    }
    this._unsentWrites.push({c: c, hints: hints});
  }

  async flush(): Promise<void> {
    if (!this._unsentWrites) {
      return;
    }

    const reqs = notNull(this._unsentWrites);
    this._unsentWrites = null;

    await this._delegate.writeBatch(reqs); // TODO: Deal with backpressure

    const self = this; // TODO: Remove this when babel bug is fixed.
    reqs.forEach(req => {
      delete self._pendingWrites[req.c.ref.toString()];
    });
  }

  async getRoot(): Promise<Ref> {
    return this._delegate.getRoot();
  }

  async updateRoot(current: Ref, last: Ref): Promise<boolean> {
    await this.flush();
    if (current.equals(last)) {
      return true;
    }

    return this._delegate.updateRoot(current, last);
  }

  // TODO: Should close() call flush() and block until it's done? Maybe closing with outstanding
  // requests should be an error on both sides. TBD.
  close() {}
}
