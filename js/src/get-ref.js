// @flow

import type Chunk from './chunk.js';
import type Ref from './ref.js';
import type DataStore from './data-store.js';
import {notNull} from './assert.js';
import type {Type} from './type.js';

type encodeFn = (v: any, t: Type, ds: ?DataStore) => Chunk;
let encodeNomsValue: ?encodeFn = null;

export function getRefOfValueOrPrimitive(v: any, t: ?Type): Ref {
  return typeof v === 'object' ? v.ref : getRef(v, notNull(t));
}

export function getRef(v: any, t: Type): Ref {
  return notNull(encodeNomsValue)(v, t, null).ref;
}

export function ensureRef(r: ?Ref, v: any, t: Type): Ref {
  if (r !== null && r !== undefined && !r.isEmpty()) {
    return r;
  }

  return getRef(v, t);
}

export function setEncodeNomsValue(encode: encodeFn) {
  encodeNomsValue = encode;
}
