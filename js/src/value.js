// @flow

import type Ref from './ref.js';
import type {primitive} from './primitives.js';
import {ensureRef} from './get-ref.js';
import type {Type} from './type.js';
import type RefValue from './ref-value.js';

export class ValueBase {
  _ref: ?Ref;

  constructor() {
    this._ref = null;
  }

  get type(): Type {
    throw new Error('abstract');
  }

  get ref(): Ref {
    return this._ref = ensureRef(this._ref, this);
  }

  get chunks(): Array<RefValue> {
    return [];
  }
}

type Value = primitive | ValueBase;
export type {Value as default};

export function getChunksOfValue(v: Value): Array<RefValue> {
  if (v instanceof ValueBase) {
    return v.chunks;
  }

  return [];
}
