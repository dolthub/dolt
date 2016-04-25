// @flow

import Ref from './ref.js';
import BatchStore from './batch-store.js';
import type {UnsentReadMap, WriteRequest} from './batch-store.js';
import type {FetchOptions} from './fetch.js';
import {deserializeChunks, serialize} from './chunk-serializer.js';
import {emptyChunk} from './chunk.js';
import {fetchArrayBuffer, fetchText} from './fetch.js';

const HTTP_STATUS_CONFLICT = 409;

type RpcStrings = {
  getRefs: string,
  root: string,
  writeValue: string,
};

const readBatchOptions = {
  method: 'POST',
  headers: {
    'Content-Type': 'application/x-www-form-urlencoded',
  },
};

export default class HttpBatchStore extends BatchStore {
  _rpc: RpcStrings;

  constructor(url: string, maxReads: number = 5, fetchOptions: FetchOptions = {}) {
    const rpc = {
      getRefs: url + '/getRefs/',
      root: url + '/root/',
      writeValue: url + '/writeValue/',
    };

    super(maxReads, new Delegate(rpc, fetchOptions));
    this._rpc = rpc;
  }
}

function mergeOptions(baseOpts: FetchOptions, opts: FetchOptions): FetchOptions {
  const hdrs = Object.assign({}, opts.headers, baseOpts.headers);
  return Object.assign({}, opts, baseOpts, {headers: hdrs});
}

export class Delegate {
  _rpc: RpcStrings;
  _readBatchOptions: FetchOptions;
  _rootOptions: FetchOptions;

  constructor(rpc: RpcStrings, fetchOptions: FetchOptions) {
    this._rpc = rpc;
    this._rootOptions = fetchOptions;
    this._readBatchOptions = mergeOptions(readBatchOptions, fetchOptions);
  }

  async readBatch(reqs: UnsentReadMap): Promise<void> {
    const refStrs = Object.keys(reqs);
    const body = refStrs.map(r => 'ref=' + r).join('&');
    const opts = Object.assign(this._readBatchOptions, {body: body});
    const buf = await fetchArrayBuffer(this._rpc.getRefs, opts);

    const chunks = deserializeChunks(buf);

    // Return success
    chunks.forEach(chunk => {
      const refStr = chunk.ref.toString();
      reqs[refStr](chunk);
      delete reqs[refStr];
    });

    // Report failure
    Object.keys(reqs).forEach(refStr => reqs[refStr](emptyChunk));
  }

  async writeBatch(reqs: Array<WriteRequest>): Promise<void> {
    const body = this._buildWriteRequest(reqs);
    await fetchText(this._rpc.writeValue, {method: 'POST', body});
  }

  _buildWriteRequest(reqs: Array<WriteRequest>): ArrayBuffer {
    let hints = new Set();
    const chunks = reqs.map(writeReq => {
      writeReq.hints.forEach(hint => { hints = hints.add(hint); });
      return writeReq.c;
    });
    return serialize(hints, chunks);
  }

  async getRoot(): Promise<Ref> {
    const refStr = await fetchText(this._rpc.root, this._rootOptions);
    return Ref.parse(refStr);
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
