/* @flow */

'use strict';

import Ref from './ref.js';
import {encodeNomsValue} from './encode.js';
import {TypeRef} from './type_ref.js';

export function getRef(v: any, t: TypeRef): Ref {
  return encodeNomsValue(v, t, null).ref;
}

export function ensureRef(r: ?Ref, v: any, t: TypeRef): Ref {
  if (r !== null && r !== undefined && !r.isEmpty()) {
    return r;
  }

  return getRef(v, t);
}
