// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import type Hash from './hash.js';
import type {primitive} from './primitives.js';
import {ensureHash} from './get-hash.js';
import type {Type} from './type.js';
import type Ref from './ref.js';

/**
 * ValueBase is the base class for non primitive Noms values.
 */
export class ValueBase {
  _hash: ?Hash;

  constructor() {
    init(this);
  }

  /**
   * The Noms type of the Noms value.
   */
  get type(): Type<any> {
    throw new Error('abstract');
  }

  /**
   * The hash of a Noms value. All Noms values have a unique hash and if two values have the same
   * hash they must be equal.
   */
  get hash(): Hash {
    return this._hash = ensureHash(this._hash, this);
  }

  /**
   * This represents the refs to the underlying chunks. If this value is a collection that has been
   * chunked then this will return the refs of th sub trees of the prolly-tree.
   */
  get chunks(): Array<Ref<any>> {
    return [];
  }
}

/**
 * Value is the union of types supported in Noms.
 */
type Value = primitive | ValueBase;
export type {Value as default};

/**
 * This returns the refs to the underlying chunks. If this value is a collection that has been
 * chunked then this will return the refs of th sub trees of the prolly-tree.
 */
export function getChunksOfValue(v: Value): Array<Ref<any>> {
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
