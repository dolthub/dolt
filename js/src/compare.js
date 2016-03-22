// @flow

import type {valueOrPrimitive, Value} from './value.js';
import {invariant} from './assert.js';

export function less(v1: any, v2: any): boolean {
  invariant(v1 !== null && v1 !== undefined && v2 !== null && v2 !== undefined);

  if (typeof v1 === 'object') {
    invariant(typeof v2 === 'object');
    return (v1:Value).less(v2);
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
    return (v1: Value).equals((v2: Value));
  }
  invariant(typeof v1 === 'string' || typeof v2 === 'number');
  invariant(typeof v1 === typeof v2);
  return v1 === v2;
}

export function compare(v1: valueOrPrimitive, v2: valueOrPrimitive): number {
  if (less(v1, v2)) {
    return -1;
  }

  return equals(v1, v2) ? 0 : 1;
}
