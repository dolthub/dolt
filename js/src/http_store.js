/* @flow */

import Chunk from './chunk.js';
import Ref from './ref.js';
import {deserialize} from './chunk_serializer.js';
import {fetchArrayBuffer, fetchText} from './fetch.js';

type ReadRequest = {
  resolve: (c: Chunk) => void,
  reject: (e: Error) => void
};

export default class HttpStore {
  _rpc: {
    getRefs: string,
    ref: string,
    root: string
  };
  _readQueue: { [key: string]: Array<ReadRequest> };
  _anyPending: boolean;
  _fetchScheduled: boolean;
  _activeReads: number;
  _maxReads: number;

  constructor(url: string, maxReads: number = 3) {
    this._rpc = {
      getRefs: url + '/getRefs/',
      ref: url + '/ref/',
      root: url + '/root/'
    };
    this._readQueue = Object.create(null);
    this._anyPending = false;
    this._fetchScheduled = false;
    this._activeReads = 0;
    this._maxReads = maxReads;
  }

  async getRoot(): Promise<Ref> {
    let refStr = await fetchText(this._rpc.root);
    return Ref.parse(refStr);
  }

  async get(ref: Ref): Promise<Chunk> {
    return new Promise((resolve, reject) => {
      let refStr = ref.toString();

      if (!this._readQueue[refStr]) {
        this._readQueue[refStr] = [];
      }

      this._readQueue[refStr].push({resolve, reject});
      this._anyPending = true;
      this._pumpFetchQueue();
    });
  }

  _pumpFetchQueue() {
    if (!this._fetchScheduled && this._anyPending && this._activeReads < this._maxReads) {
      this._fetchScheduled = true;
      setTimeout(() => {
        this._beginFetch();
      }, 0);
    }
  }

  async _beginFetch(): Promise<void> {
    this._activeReads++;
    let reqs = this._readQueue;
    this._readQueue = Object.create(null);
    this._anyPending = false;
    this._fetchScheduled = false;
    let refStrs = Object.keys(reqs);
    let body = refStrs.map(r => 'ref=' + r).join('&');

    try {
      let buffer = await fetchArrayBuffer(this._rpc.getRefs, {
        method: 'post',
        body: body,
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded'
        }
      });

      let chunks = deserialize(buffer);

      // Return success
      chunks.forEach(chunk => {
        let refStr = chunk.ref.toString();
        let callers = reqs[refStr];
        delete reqs[refStr];
        callers.forEach(caller => {
          caller.resolve(chunk);
        });
      });

      // Report failure
      Object.keys(reqs).forEach(r => {
        let callers = reqs[r];
        callers.forEach(c => {
          c.reject(new Chunk());
        });
      });
    } catch (err) {
      // TODO: This is fatal.
      throw err;
    } finally {
      this._endFetch();
    }
  }

  _endFetch() {
    this._activeReads--;
    this._pumpFetchQueue();
  }

  close() {}
}
