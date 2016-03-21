// @flow

import Chunk from './chunk.js';
import Ref from './ref.js';
import {invariant, notNull} from './assert.js';
import {Type} from './type.js';
import type DataStore from './data-store.js';

type encodeFn = (v: any, t: Type, ds: ?DataStore) => Chunk;
let encodeNomsValue: ?encodeFn = null;

export function getRefOfValueOrPrimitive(v: any, t: ?Type): Ref {
  if (v.ref instanceof Ref) {
    return v.ref;
  }

  invariant(t);
  return getRef(v, t);
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
