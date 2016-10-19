// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {alloc, compare, sha512} from './bytes.js';
import {encode, decode} from './base32';

export const byteLength = 20;
export const stringLength = 32;
const pattern = /^[0-9a-v]{32}$/;

/**
 * Hash is used to represent the hash of a Noms Value.
 */
export default class Hash {
  _digest: Uint8Array;

  /**
   * The Hash instance does not copy the `digest` so if the `digest` is part of a large ArrayBuffer
   * the caller might want to make a copy first to prevent that ArrayBuffer from being retained.
   */
  constructor(digest: Uint8Array) {
    this._digest = digest;
  }

  /**
   * The underlying byte array that represents the hash.
   */
  get digest(): Uint8Array {
    return this._digest;
  }

  /**
   * Whether this Hash object is equal to the empty hash.
   */
  isEmpty(): boolean {
    return this.equals(emptyHash);
  }

  /**
   * If this hash is equal to `other` then this returns `true`.
   */
  equals(other: Hash): boolean {
    return this.compare(other) === 0;
  }

  /**
   * Compares two hashes. This returns < 0 when this hash is smaller than `other`, 0 if they are
   * equal and > 0 if this hash is larger than `other`.
   */
  compare(other: Hash): number {
    return compare(this._digest, other._digest);
  }

  /**
   * Returns a Base32 encoded version of the hash.
   */
  toString(): string {
    return encode(this._digest);
  }

  /**
   * Parses a string representing a hash as a Base32 encoded byte array.
   * If the string is not well formed then this returns `null`.
   */
  static parse(s: string): Hash | null {
    if (pattern.test(s)) {
      return new Hash(decode(s));
    }
    return null;
  }

  /**
   * Computes the hash from `data`.
   */
  static fromData(data: Uint8Array): Hash {
    return new Hash(sha512(data));
  }
}

/**
 * The empty hash (all zeroes).
 */
export const emptyHash: Hash = new Hash(alloc(byteLength));
