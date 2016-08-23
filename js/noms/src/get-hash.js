// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import type Chunk from './chunk.js';
import type Hash from './hash.js';
import type {ValueWriter} from './value-store.js';
import {notNull} from './assert.js';
import type Value from './value.js';
import {getTypeOfValue} from './type.js';
import {ValueBase} from './value.js';

type encodeFn = (v: Value, vw: ?ValueWriter) => Chunk;
let encodeValue: ?encodeFn = null;

export function getHashOfValue(v: Value): Hash {
  if (v instanceof ValueBase) {
    return v.hash;
  }

  return getHash(v, getTypeOfValue(v));
}

export function getHash(v: Value): Hash {
  return notNull(encodeValue)(v, null).hash;
}

export function ensureHash(h: ?Hash, v: Value): Hash {
  if (h !== null && h !== undefined && !h.isEmpty()) {
    return h;
  }

  return getHash(v);
}

export function setEncodeValue(encode: encodeFn) {
  encodeValue = encode;
}
