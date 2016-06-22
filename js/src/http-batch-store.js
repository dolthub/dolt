// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import Hash from './hash.js';
import RemoteBatchStore from './remote-batch-store.js';
import type {UnsentReadMap} from './remote-batch-store.js';
import type {FetchOptions} from './fetch.js';
import type {ChunkStream} from './chunk-serializer.js';
import {serialize, deserializeChunks} from './chunk-serializer.js';
import {emptyChunk} from './chunk.js';
import {fetchUint8Array, fetchText} from './fetch.js';
import {notNull} from './assert.js';
import {NomsVersion} from './version.js';

const HTTP_STATUS_CONFLICT = 409;
const VersionHeader = 'X-Noms-Vers';

type RpcStrings = {
  getRefs: string,
  root: string,
  writeValue: string,
};

const versOptions = {
  headers: {
    VersionHeader: NomsVersion,
  },
  respHeaders: [VersionHeader],
};

const readBatchOptions = {
  method: 'POST',
  headers: {
    'Content-Type': 'application/x-www-form-urlencoded',
  },
};

export default class HttpBatchStore extends RemoteBatchStore {
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
    this._rootOptions = mergeOptions(versOptions, fetchOptions);
    this._readBatchOptions = mergeOptions(readBatchOptions, this._rootOptions);
    this._body = new ArrayBuffer(0);
  }

  async readBatch(reqs: UnsentReadMap): Promise<void> {
    const hashStrs = Object.keys(reqs);
    const body = hashStrs.map(r => 'ref=' + r).join('&');
    const opts = Object.assign(this._readBatchOptions, {body: body});
    const {headers, buf} = await fetchUint8Array(this._rpc.getRefs, opts);

    const versionErr = checkVersion(headers);
    if (versionErr) {
      return Promise.reject(versionErr);
    }

    const chunks = deserializeChunks(buf, new DataView(buf.buffer, buf.byteOffset, buf.byteLength));

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
      .then(({headers}) => {
        const versionErr = checkVersion(headers);
        if (versionErr) {
          return Promise.reject(versionErr);
        }
      });
  }

  async getRoot(): Promise<Hash> {
    const {headers, buf} = await fetchText(this._rpc.root, this._rootOptions);
    const versionErr = checkVersion(headers);
    if (versionErr) {
      return Promise.reject(versionErr);
    }
    return notNull(Hash.parse(buf));
  }

  async updateRoot(current: Hash, last: Hash): Promise<boolean> {
    const ch = this._rpc.root.indexOf('?') >= 0 ? '&' : '?';
    const params = `${ch}current=${current}&last=${last}`;
    try {
      const {headers} = await fetchText(this._rpc.root + params, {method: 'POST'});
      const versionErr = checkVersion(headers);
      if (versionErr) {
        return Promise.reject(versionErr);
      }
      return true;
    } catch (ex) {
      if (ex === HTTP_STATUS_CONFLICT) {
        return false;
      }
      throw ex;
    }
  }
}

function checkVersion(headers: {[key: string]: string}): ?Error {
  const vers = headers[VersionHeader];
  if (vers !== NomsVersion) {
    return new Error(
      `SDK version ${NomsVersion} is not compatible with data of version ${vers}.`);
  }
  return null;
}
