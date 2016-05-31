// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import Hash from './hash.js';
import BatchStore from './batch-store.js';
import type {UnsentReadMap} from './batch-store.js';
import type {FetchOptions} from './fetch.js';
import type {ChunkStream} from './chunk-serializer.js';
import {serialize, deserializeChunks} from './chunk-serializer.js';
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

  constructor(urlparam: string, maxReads: number = 5, fetchOptions: FetchOptions = {}) {
    const [url, params] = separateParams(urlparam);
    const rpc = {
      getRefs: url + '/getRefs/' + params,
      root: url + '/root/' + params,
      writeValue: url + '/writeValue/' + params,
    };

    super(maxReads, new Delegate(rpc, fetchOptions));
    this._rpc = rpc;
  }
}

function separateParams(url: string): [string, string] {
  let u = url;
  let params = '';
  const m = url.match(/^(.+?)(\?.+)?$/);
  if (!m) {
    throw new Error('Could not parse url: ' + url);
  }
  if (m[2]) {
    [, u, params] = m;
  }
  u = u.replace(/\/*$/, '');
  return [u, params];
}

function mergeOptions(baseOpts: FetchOptions, opts: FetchOptions): FetchOptions {
  const hdrs = Object.assign({}, opts.headers, baseOpts.headers);
  return Object.assign({}, opts, baseOpts, {headers: hdrs});
}

export class Delegate {
  _rpc: RpcStrings;
  _readBatchOptions: FetchOptions;
  _rootOptions: FetchOptions;
  _body: ArrayBuffer;

  constructor(rpc: RpcStrings, fetchOptions: FetchOptions) {
    this._rpc = rpc;
    this._rootOptions = fetchOptions;
    this._readBatchOptions = mergeOptions(readBatchOptions, fetchOptions);
    this._body = new ArrayBuffer(0);
  }

  async readBatch(reqs: UnsentReadMap): Promise<void> {
    const hashStrs = Object.keys(reqs);
    const body = hashStrs.map(r => 'ref=' + r).join('&');
    const opts = Object.assign(this._readBatchOptions, {body: body});
    const buf = await fetchArrayBuffer(this._rpc.getRefs, opts);

    const chunks = deserializeChunks(buf);

    // Return success
    chunks.forEach(chunk => {
      const hashStr = chunk.hash.toString();
      reqs[hashStr](chunk);
      delete reqs[hashStr];
    });

    // Report failure
    Object.keys(reqs).forEach(hashStr => reqs[hashStr](emptyChunk));
  }

  writeBatch(hints: Set<Hash>, chunkStream: ChunkStream): Promise<void> {
    return serialize(hints, chunkStream)
      .then(body => fetchText(this._rpc.writeValue, {method: 'POST', body}))
      .then(() => undefined);
  }

  async getRoot(): Promise<Hash> {
    const hashStr = await fetchText(this._rpc.root, this._rootOptions);
    return Hash.parse(hashStr);
  }

  async updateRoot(current: Hash, last: Hash): Promise<boolean> {
    const ch = this._rpc.root.indexOf('?') >= 0 ? '&' : '?';
    const params = `${ch}current=${current}&last=${last}`;
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
