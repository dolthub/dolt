// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import Bytes from './bytes.js';

export const sha1Size = 20;
const pattern = /^sha1-[0-9a-f]{40}$/;

export default class Hash {
  _digest: Uint8Array;

  /**
   * The Hash instance does not copy the `digest` so if the `digest` is part of a large ArrayBuffer
   * the caller might want to make a copy first to prevent that ArrayBuffer from being retained.
   */
  constructor(digest: Uint8Array) {
    this._digest = digest;
  }

  get digest(): Uint8Array {
    return this._digest;
  }

  isEmpty(): boolean {
    return this.equals(emptyHash);
  }

  equals(other: Hash): boolean {
    return this.compare(other) === 0;
  }

  compare(other: Hash): number {
    return Bytes.compare(this._digest, other._digest);
  }

  toString(): string {
    return 'sha1-' + Bytes.toHexString(this._digest);
  }

  static parse(s: string): ?Hash {
    if (pattern.test(s)) {
      return new Hash(Bytes.fromHexString(s.substring(5)));
    }
    return null;
  }

  static fromData(data: Uint8Array): Hash {
    return new Hash(Bytes.sha1(data));
  }
}

export const emptyHash = new Hash(Bytes.alloc(sha1Size));
