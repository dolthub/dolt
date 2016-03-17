// @flow

import Chunk from './chunk.js';
import Ref from './ref.js';
import {notNull} from './assert.js';

type PendingReadMap = { [key: string]: Promise<Chunk> };
export type UnsentReadMap = { [key: string]: (c: Chunk) => void };

export type WriteMap = { [key: string]: Chunk };

export class RemoteStore {
  _pendingReads: PendingReadMap;
  _unsentReads: ?UnsentReadMap;
  _readScheduled: boolean;
  _activeReads: number;
  _maxReads: number;

  _pendingWrites: WriteMap;
  _unsentWrites: ?WriteMap;
  _writeScheduled: boolean;
  _activeWrites: number;
  _maxWrites: number;
  _allWritesFinishedFn: ?() => void;
  _canUpdateRoot: Promise<void>;

  constructor(maxReads: number, maxWrites: number) {
    this._pendingReads = Object.create(null);
    this._unsentReads = null;
    this._readScheduled = false;
    this._activeReads = 0;
    this._maxReads = maxReads;

    this._pendingWrites = Object.create(null);
    this._unsentWrites = null;
    this._writeScheduled = false;
    this._activeWrites = 0;
    this._maxWrites = maxWrites;
    this._allWritesFinishedFn = null;
    this._canUpdateRoot = Promise.resolve();
  }

  get(ref: Ref): Promise<Chunk> {
    const refStr = ref.toString();
    const p = this._pendingReads[refStr];
    if (p) {
      return p;
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

    await this.internalReadBatch(reqs);

    const self = this; // TODO: Remove this when babel bug is fixed.
    Object.keys(reqs).forEach(refStr => {
      delete self._pendingReads[refStr];
    });

    this._activeReads--;
    this._maybeStartRead();
  }

  async internalReadBatch(reqs: UnsentReadMap): // eslint-disable-line no-unused-vars
      Promise<void> {
    throw new Error('override');
  }

  put(c: Chunk): void {
    const refStr = c.ref.toString();
    if (this._pendingWrites[refStr]) {
      return; // Already in flight.
    }
    this._pendingWrites[refStr] = c;

    if (!this._unsentWrites) {
      this._unsentWrites = Object.create(null);
    }
    this._unsentWrites[refStr] = c;

    if (!this._allWritesFinishedFn) {
      this._canUpdateRoot = new Promise(resolve => {
        this._allWritesFinishedFn = resolve;
      });
    }

    this._maybeStartWrite();
  }

  _maybeStartWrite() {
    if (!this._writeScheduled && this._unsentWrites && this._activeWrites < this._maxWrites) {
      this._writeScheduled = true;
      setTimeout(() => {
        this._write();
      }, 0);
    }
  }

  async _write(): Promise<void> {
    this._activeWrites++;

    const reqs = notNull(this._unsentWrites);
    this._unsentWrites = null;
    this._writeScheduled = false;

    await this.internalWriteBatch(reqs);

    const self = this; // TODO: Remove this when babel bug is fixed.
    Object.keys(reqs).forEach(refStr => {
      delete self._pendingWrites[refStr];
    });

    this._activeWrites--;

    if (this._activeWrites === 0 && !this._unsentWrites) {
      notNull(this._allWritesFinishedFn)();
      this._allWritesFinishedFn = null;
    }

    this._maybeStartWrite();
  }

  async internalWriteBatch(reqs: WriteMap): Promise<void> { // eslint-disable-line no-unused-vars
    throw new Error('override');
  }

  async updateRoot(current: Ref, last: Ref): Promise<boolean> {
    await this._canUpdateRoot;
    if (current.equals(last)) {
      return Promise.resolve(true);
    }

    return this.internalUpdateRoot(current, last);
  }

  internalUpdateRoot(current: Ref, last: Ref): // eslint-disable-line no-unused-vars
      Promise<boolean> {
    throw new Error('override');
  }

  close() {}
}
