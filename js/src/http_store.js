// @flow

import Ref from './ref.js';
import type {UnsentReadMap} from './remote_store.js';
import {deserialize} from './chunk_serializer.js';
import {emptyChunk, default as Chunk} from './chunk.js';
import type {FetchOptions} from './fetch.js';
import {fetchArrayBuffer, fetchText} from './fetch.js';
import {RemoteStore} from './remote_store.js';

export default class HttpStore extends RemoteStore {
  _rpc: {
    getRefs: string,
    root: string
  };
  _fetchOptions: FetchOptions;

  constructor(url: string, maxReads: number = 3, fetchOptions: FetchOptions = {}) {
    super(maxReads);

    this._rpc = {
      getRefs: url + '/getRefs/',
      root: url + '/root/',
    };
    this._fetchOptions = fetchOptions;
  }

  async getRoot(): Promise<Ref> {
    const refStr = await fetchText(this._rpc.root, this._fetchOptions);
    return Ref.parse(refStr);
  }

  updateRoot(current: Ref, last: Ref): Promise<boolean> {  // eslint-disable-line
    throw new Error('not implemented');
  }

  put(c: Chunk): void {  // eslint-disable-line
    throw new Error('not implemented');
  }

  has(ref: Ref): Promise<boolean> {  // eslint-disable-line
    throw new Error('not implemented');
  }

  async _readBatch(reqs: UnsentReadMap): Promise<void> {
    const refStrs = Object.keys(reqs);
    const body = refStrs.map(r => 'ref=' + r).join('&');
    const opts: FetchOptions = {
      method: 'post',
      body: body,
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
    };
    const buf = await fetchArrayBuffer(this._rpc.getRefs, Object.assign(opts, this._fetchOptions));

    const chunks = deserialize(buf);

    // Return success
    chunks.forEach(chunk => {
      const refStr = chunk.ref.toString();
      reqs[refStr](chunk);
      delete reqs[refStr];
    });

    // Report failure
    Object.keys(reqs).forEach(refStr => reqs[refStr](emptyChunk));
  }
}
