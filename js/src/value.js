// @flow

import type Hash from './hash.js';
import type {primitive} from './primitives.js';
import {ensureHash} from './get-hash.js';
import type {Type} from './type.js';
import type Ref from './ref.js';

export class ValueBase {
  _hash: ?Hash;

  constructor() {
    init(this);
  }

  get type(): Type {
    throw new Error('abstract');
  }

  get hash(): Hash {
    return this._hash = ensureHash(this._hash, this);
  }

  get chunks(): Array<Ref> {
    return [];
  }
}

type Value = primitive | ValueBase;
export type {Value as default};

export function getChunksOfValue(v: Value): Array<Ref> {
  if (v instanceof ValueBase) {
    return v.chunks;
  }

  return [];
}

export function init(v: ValueBase) {
  v._hash = null;
}

// For internal use only. Do not export this from noms.js
export function setHash(v: ValueBase, h: Hash) {
  v._hash = h;
}
