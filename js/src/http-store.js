// @flow

import Ref from './ref.js';
import type {FetchOptions} from './fetch.js';
import type {UnsentReadMap} from './remote-store.js';
import type {WriteMap} from './remote-store.js';
import {deserialize, serialize} from './chunk-serializer.js';
import {emptyChunk} from './chunk.js';
import {fetchArrayBuffer, fetchText} from './fetch.js';
import {RemoteStore} from './remote-store.js';

const HTTP_STATUS_CONFLICT = 409;

type RpcStrings = {
  getRefs: string,
  postRefs: string,
  root: string,
};

const readBatchOptions = {
  method: 'POST',
  headers: {
    'Content-Type': 'application/x-www-form-urlencoded',
  },
};

export default class HttpStore extends RemoteStore {
  _rpc: RpcStrings;
  _rootOptions: FetchOptions;

  constructor(url: string, maxReads: number = 3, maxWrites: number = 3,
      fetchOptions: FetchOptions = {}) {
    const rpc = {
      getRefs: url + '/getRefs/',
      postRefs: url + '/postRefs/',
      root: url + '/root/',
    };

    const mergedOptions = mergeOptions(readBatchOptions, fetchOptions);
    super(maxReads, maxWrites, new Delegate(rpc, mergedOptions));
    this._rpc = rpc;
    this._rootOptions = fetchOptions;
  }

  async getRoot(): Promise<Ref> {
    const refStr = await fetchText(this._rpc.root, this._rootOptions);
    return Ref.parse(refStr);
  }

  has(ref: Ref): Promise<boolean> {  // eslint-disable-line no-unused-vars
    throw new Error('not implemented');
  }
}

function mergeOptions(baseOpts: FetchOptions, opts: FetchOptions): FetchOptions {
  const hdrs = Object.assign({}, opts.headers, baseOpts.headers);
  return Object.assign({}, opts, baseOpts, {headers: hdrs});
}

class Delegate {
  _rpc: RpcStrings;
  _readBatchOptions: FetchOptions;

  constructor(rpc: RpcStrings, readBatchOptions: FetchOptions) {
    this._rpc = rpc;
    this._readBatchOptions = readBatchOptions;
  }

  async readBatch(reqs: UnsentReadMap): Promise<void> {
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

  async writeBatch(reqs: WriteMap): Promise<void> {
    const chunks = Object.keys(reqs).map(refStr => reqs[refStr]);
    const body = serialize(chunks);
    await fetchText(this._rpc.postRefs, {method: 'POST', body});
  }

  async updateRoot(current: Ref, last: Ref): Promise<boolean> {
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
}
