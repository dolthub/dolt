// @flow

import type Chunk from './chunk.js';
import type Ref from './ref.js';
import type {ValueWriter} from './value-store.js';
import {notNull} from './assert.js';
import type {valueOrPrimitive} from './value.js';
import {getTypeOfValue} from './type.js';
import {Value} from './value.js';

type encodeFn = (v: valueOrPrimitive, vw: ?ValueWriter) => Chunk;
let encodeNomsValue: ?encodeFn = null;

export function getRefOfValue(v: valueOrPrimitive): Ref {
  if (v instanceof Value) {
    return v.ref;
  }

  return getRef(v, getTypeOfValue(v));
}

export function getRef(v: valueOrPrimitive): Ref {
  return notNull(encodeNomsValue)(v, null).ref;
}

export function ensureRef(r: ?Ref, v: valueOrPrimitive): Ref {
  if (r !== null && r !== undefined && !r.isEmpty()) {
    return r;
  }

  return getRef(v);
}

export function setEncodeNomsValue(encode: encodeFn) {
  encodeNomsValue = encode;
}
