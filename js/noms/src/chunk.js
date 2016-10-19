// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import Hash from './hash.js';
import * as Bytes from './bytes.js';

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
    return Bytes.toString(this.data);
  }

  static fromString(s: string, hash: ?Hash): Chunk {
    return new Chunk(Bytes.fromString(s), hash);
  }

  static emptyChunk: Chunk;
}

export const emptyChunk = new Chunk(Bytes.alloc(0));
