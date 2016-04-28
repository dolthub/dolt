// @flow

import type Chunk from './chunk.js';
import type Ref from './ref.js';
import type DataStore from './data-store.js';
import {notNull} from './assert.js';
import type {Type} from './type.js';
import type {valueOrPrimitive} from './value.js';
import {getTypeOfValue} from './type.js';
import {Value} from './value.js';

type encodeFn = (v: valueOrPrimitive, t: Type, ds: ?DataStore) => Chunk;
let encodeNomsValue: ?encodeFn = null;

export function getRefOfValue(v: valueOrPrimitive): Ref {
  if (v instanceof Value) {
    return v.ref;
  }

  return getRef(v, getTypeOfValue(v));
}

export function getRef(v: valueOrPrimitive, t: Type): Ref {
  return notNull(encodeNomsValue)(v, t, null).ref;
}

export function ensureRef(r: ?Ref, v: valueOrPrimitive, t: Type): Ref {
  if (r !== null && r !== undefined && !r.isEmpty()) {
    return r;
  }

  return getRef(v, t);
}

export function setEncodeNomsValue(encode: encodeFn) {
  encodeNomsValue = encode;
}
