/* @flow */

import Ref from './ref.js';
import {encodeNomsValue} from './encode.js';
import {Type} from './type.js';

export function getRef(v: any, t: Type): Ref {
  return encodeNomsValue(v, t, null).ref;
}

export function ensureRef(r: ?Ref, v: any, t: Type): Ref {
  if (r !== null && r !== undefined && !r.isEmpty()) {
    return r;
  }

  return getRef(v, t);
}
