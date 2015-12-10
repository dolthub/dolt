// @flow

import Ref from './ref.js';
import type {primitive} from './primitives.js';
import {ensureRef} from './get_ref.js';
import {invariant} from './assert.js';
import {Type} from './type.js';

export class Value {
  type: Type;
  _ref: ?Ref;

  constructor(type: Type) {
    this.type = type;
    this._ref = null;
  }

  get ref(): Ref {
    return ensureRef(this._ref, this, this.type);
  }

  equals(other: Value): boolean {
    return this.ref.equals(other.ref);
  }
}

export type valueOrPrimitive = Value | primitive;

export function less(v1: any, v2: any): boolean {
  invariant(v1 !== null && v1 !== undefined && v2 !== null && v2 !== undefined);

  if (v1 instanceof Ref) {
    invariant(v2 instanceof Ref);
    return v1.compare(v2) < 0;
  }

  if (typeof v1 === 'object') {
    invariant(v1.ref instanceof Ref);
    invariant(v2.ref instanceof Ref);
    return v1.ref.compare(v2.ref) < 0;
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
