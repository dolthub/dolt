// @flow

import Chunk from './chunk.js';
import Ref from './ref.js';
import {notNull} from './assert.js';

type PendingReadMap = { [key: string]: Promise<Chunk> };
export type UnsentReadMap = { [key: string]: (c: Chunk) => void };

export class RemoteStore {
  _pendingReads: PendingReadMap;
  _unsentReads: ?UnsentReadMap;
  _readScheduled: boolean;
  _activeReads: number;
  _maxReads: number;

  constructor(maxReads: number) {
    this._pendingReads = Object.create(null);
    this._unsentReads = null;
    this._readScheduled = false;
    this._activeReads = 0;
    this._maxReads = maxReads;
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

    await this._readBatch(reqs);

    const self = this;
    Object.keys(reqs).forEach(refStr => {
      delete self._pendingReads[refStr];
    });

    this._activeReads--;
    this._maybeStartRead();
  }

  async _readBatch(reqs: UnsentReadMap): Promise<void> { // eslint-disable-line no-unused-vars
    throw new Error('override');
  }

  close() {}
}
