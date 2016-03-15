// @flow

import Ref from './ref.js';
import type {FetchOptions} from './fetch.js';
import type {UnsentReadMap} from './remote_store.js';
import type {WriteMap} from './remote_store.js';
import {deserialize, serialize} from './chunk_serializer.js';
import {emptyChunk} from './chunk.js';
import {fetchArrayBuffer, fetchText} from './fetch.js';
import {RemoteStore} from './remote_store.js';

const HTTP_STATUS_CONFLICT = 409;

const readBatchOptions = {
  method: 'POST',
  headers: {
    'Content-Type': 'application/x-www-form-urlencoded',
  },
};

export default class HttpStore extends RemoteStore {
  _rpc: {
    getRefs: string,
    postRefs: string,
    root: string,
  };
  _rootOptions: FetchOptions;
  _readBatchOptions: FetchOptions;

  constructor(url: string, maxReads: number = 3, maxWrites: number = 3,
      fetchOptions: FetchOptions = {}) {
    super(maxReads, maxWrites);

    this._rpc = {
      getRefs: url + '/getRefs/',
      postRefs: url + '/postRefs/',
      root: url + '/root/',
    };
    this._rootOptions = fetchOptions;
    this._readBatchOptions = this._mergeOptions(readBatchOptions, fetchOptions);
  }

  async getRoot(): Promise<Ref> {
    const refStr = await fetchText(this._rpc.root, this._rootOptions);
    return Ref.parse(refStr);
  }

  async internalUpdateRoot(current: Ref, last: Ref): Promise<boolean> {
    const params = `?current=${current}&last=${last}`;
    try {
      await fetchText(this._rpc.root + params, {method: 'POST'});
      return true;
    } catch (ex) {
      if (ex === HTTP_STATUS_CONFLICT) {
        return false;
      }
      throw ex;
    }
  }

  has(ref: Ref): Promise<boolean> {  // eslint-disable-line
    throw new Error('not implemented');
  }

  _mergeOptions(baseOpts: FetchOptions, opts: FetchOptions): FetchOptions {
    const hdrs = Object.assign({}, opts.headers, baseOpts.headers);
    return Object.assign({}, opts, baseOpts, {headers: hdrs});
  }

  async internalReadBatchInternal(reqs: UnsentReadMap): Promise<void> {
    const refStrs = Object.keys(reqs);
    const body = refStrs.map(r => 'ref=' + r).join('&');
    const opts = Object.assign(this._readBatchOptions, {body: body});
    const buf = await fetchArrayBuffer(this._rpc.getRefs, opts);

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

  async internalWriteBatch(reqs: WriteMap): Promise<void> {
    const chunks = Object.keys(reqs).map(refStr => reqs[refStr]);
    const body = serialize(chunks);
    await fetchText(this._rpc.postRefs, {method: 'POST', body});
  }
}
