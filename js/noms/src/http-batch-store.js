// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import Hash from './hash.js';
import RemoteBatchStore from './remote-batch-store.js';
import type {UnsentReadMap} from './remote-batch-store.js';
import type {FetchOptions} from './fetch.js';
import type {ChunkStream} from './chunk-serializer.js';
import {serialize, deserializeChunks} from './chunk-serializer.js';
import {emptyChunk} from './chunk.js';
import {
  fetchUint8Array as fetchUint8ArrayWithoutVersion,
  fetchText as fetchTextWithoutVersion,
} from './fetch.js';
import HttpError from './http-error.js';
import {notNull} from './assert.js';
import nomsVersion from './version.js';

export const DEFAULT_MAX_READS = 5;
const HTTP_STATUS_CONFLICT = 409;
const versionHeader = 'x-noms-vers';

type RpcStrings = {
  getRefs: string,
  root: string,
  writeValue: string,
};

const versionOptions = {
  headers: {
    [versionHeader]: nomsVersion,
  },
};

function fetchText(url: string, options: FetchOptions) {
  return fetchTextWithoutVersion(url, mergeOptions(options, versionOptions));
}

function fetchUint8Array(url: string, options: FetchOptions) {
  return fetchUint8ArrayWithoutVersion(url, mergeOptions(options, versionOptions));
}

const readBatchOptions = {
  method: 'POST',
  headers: {
    'Content-Type': 'application/x-www-form-urlencoded',
  },
};

export default class HttpBatchStore extends RemoteBatchStore {
  _rpc: RpcStrings;

  constructor(
      urlparam: string, maxReads: number = DEFAULT_MAX_READS, fetchOptions: FetchOptions = {}) {
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
    const {headers, buf} = await fetchUint8Array(this._rpc.getRefs, opts);

    const versionErr = checkVersion(headers);
    if (versionErr) {
      return Promise.reject(versionErr);
    }

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
    const params = `${ch}current=${current.toString()}&last=${last.toString()}`;
    try {
      const {headers} = await fetchText(this._rpc.root + params, {method: 'POST'});
      const versionErr = checkVersion(headers);
      if (versionErr) {
        return Promise.reject(versionErr);
      }
      return true;
    } catch (ex) {
      if (ex instanceof HttpError && ex.status === HTTP_STATUS_CONFLICT) {
        return false;
      }
      throw ex;
    }
  }
}

function checkVersion(headers: Map<string, string>): ?Error {
  const version = headers.get(versionHeader);
  if (version !== nomsVersion) {
    return new Error(
      `SDK version ${nomsVersion} is not compatible with data of version ${String(version)}.`);
  }
  return null;
}
