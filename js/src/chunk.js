// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import Hash from './hash.js';
import {encode, decode} from './utf8.js';

export default class Chunk {
  data: Uint8Array;
  _hash: ?Hash;

  constructor(data: Uint8Array, hash: ?Hash) {
    this.data = data;
    this._hash = hash;
  }

  get hash(): Hash {
    return this._hash || (this._hash = Hash.fromData(this.data));
  }

  isEmpty(): boolean {
    return this.data.length === 0;
  }

  toString(): string {
    return decode(this.data);
  }

  static emptyChunk: Chunk;

  static fromString(s: string, hash: ?Hash): Chunk {
    return new Chunk(encode(s), hash);
  }
}

export const emptyChunk = new Chunk(new Uint8Array(0));
