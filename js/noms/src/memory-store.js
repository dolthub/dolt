// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import type Hash from './hash.js';
import {emptyHash} from './hash.js';
import type Chunk from './chunk.js';
import {emptyChunk} from './chunk.js';

export default class MemoryStore {
  _data: { [key: string]: Chunk };
  _root: Hash;

  constructor() {
    this._data = Object.create(null);
    this._root = emptyHash;
  }

  getRoot(): Promise<Hash> {
    return Promise.resolve(this._root);
  }

  updateRoot(current: Hash, last: Hash): Promise<boolean> {
    if (!this._root.equals(last)) {
      return Promise.resolve(false);
    }

    this._root = current;
    return Promise.resolve(true);
  }

  get(hash: Hash): Promise<Chunk> {
    let c = this._data[hash.toString()];
    if (!c) {
      c = emptyChunk;
    }

    return Promise.resolve(c);
  }

  has(hash: Hash): Promise<boolean> {
    return Promise.resolve(this._data[hash.toString()] !== undefined);
  }

  put(c: Chunk) {
    this._data[c.hash.toString()] = c;
  }

  get size(): number {
    return Object.keys(this._data).length;
  }

  close() {}
}
