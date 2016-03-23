// @flow

import Ref from './ref.js';
import type {primitive} from './primitives.js';
import {ensureRef} from './get-ref.js';
import {Type} from './type.js';

export interface Value {
  ref: Ref;
  equals(other: Value): boolean;
  less(other: Value): boolean;
  chunks: Array<Ref>;
  type: Type;
}

export class ValueBase {
  type: Type;
  _ref: ?Ref;

  constructor(type: Type) {
    this.type = type;
    this._ref = null;
  }

  get ref(): Ref {
    return this._ref = ensureRef(this._ref, this, this.type);
  }

  equals(other: Value): boolean {
    return this.ref.equals(other.ref);
  }

  less(other: Value): boolean {
    return this.ref.less(other.ref);
  }
}

export type valueOrPrimitive = primitive | Value;
