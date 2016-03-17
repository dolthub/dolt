// @flow

import Ref from './ref.js';
import type {primitive} from './primitives.js';
import {ensureRef} from './get-ref.js';
import {invariant} from './assert.js';
import {Type} from './type.js';

export type Value = {
  ref: Ref;
  equals(other: Value): boolean;
  chunks: Array<Ref>;
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
}

export type valueOrPrimitive = Value | primitive;

export function less(v1: any, v2: any): boolean {
  invariant(v1 !== null && v1 !== undefined && v2 !== null && v2 !== undefined);

  if (typeof v1 === 'object') {
    invariant(typeof v2 === 'object');
    if (v1 instanceof ValueBase) {
      v1 = v1.ref;
    }
    if (v2 instanceof ValueBase) {
      v2 = v2.ref;
    }

    invariant(v1 instanceof Ref);
    invariant(v2 instanceof Ref);
    return v1.less(v2);
  }

  if (typeof v1 === 'string') {
    invariant(typeof v2 === 'string');
    return v1 < v2;
  }

  invariant(typeof v1 === 'number');
  invariant(typeof v2 === 'number');
  return v1 < v2;
}

export function equals(v1: valueOrPrimitive, v2: valueOrPrimitive): boolean {
  invariant(v1 !== null && v1 !== undefined && v2 !== null && v2 !== undefined);

  if (typeof v1 === 'object') {
    invariant(typeof v2 === 'object');
    return (v1:Value).equals((v2:Value));
  }

  if (typeof v1 === 'string') {
    invariant(typeof v2 === 'string');
    return v1 === v2;
  }

  invariant(typeof v1 === 'number');
  invariant(typeof v2 === 'number');
  return v1 === v2;
}

export function compare(v1: valueOrPrimitive, v2: valueOrPrimitive): number {
  if (less(v1, v2)) {
    return -1;
  }

  return equals(v1, v2) ? 0 : 1;
}
