// @flow

import type Chunk from './chunk.js';
import type Ref from './ref.js';
import type {ValueWriter} from './value-store.js';
import {notNull} from './assert.js';
import type Value from './value.js';
import {getTypeOfValue} from './type.js';
import {ValueBase} from './value.js';

type encodeFn = (v: Value, vw: ?ValueWriter) => Chunk;
let encodeNomsValue: ?encodeFn = null;

export function getRefOfValue(v: Value): Ref {
  if (v instanceof ValueBase) {
    return v.ref;
  }

  return getRef(v, getTypeOfValue(v));
}

export function getRef(v: Value): Ref {
  return notNull(encodeNomsValue)(v, null).ref;
}

export function ensureRef(r: ?Ref, v: Value): Ref {
  if (r !== null && r !== undefined && !r.isEmpty()) {
    return r;
  }

  return getRef(v);
}

export function setEncodeNomsValue(encode: encodeFn) {
  encodeNomsValue = encode;
}
