// @flow

import type Hash from './hash.js';
import type {primitive} from './primitives.js';
import {ensureHash} from './get-hash.js';
import type {Type} from './type.js';
import type RefValue from './ref-value.js';

export class ValueBase {
  _hash: ?Hash;

  constructor() {
    this._hash = null;
  }

  get type(): Type {
    throw new Error('abstract');
  }

  get hash(): Hash {
    return this._hash = ensureHash(this._hash, this);
  }

  get chunks(): Array<RefValue> {
    return [];
  }
}

type Value = primitive | ValueBase;
export type {Value as default};

export function getChunksOfValue(v: Value): Array<RefValue> {
  if (v instanceof ValueBase) {
    return v.chunks;
  }

  return [];
}
