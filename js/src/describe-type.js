// @flow

import {ValueBase} from './value.js';

export default function describeType(v: any): string {
  const t = typeof v;
  if (t === 'object') {
    if (v === null) {
      return 'null';
    }
    if (v instanceof ValueBase) {
      return v.type.describe();
    }
  }
  return t;
}
