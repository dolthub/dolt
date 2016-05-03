// @flow

import type {Type} from './type.js';
import type {valueOrPrimitive} from './value.js';
import {Kind} from './noms-kind.js';
import {Value} from './value.js';
import {getRef} from './get-ref.js';
import {invariant} from './assert.js';

// TODO: Implement total ordering of noms types.
export function less(v1: valueOrPrimitive, v2: valueOrPrimitive): boolean {
  invariant(typeof v1 === typeof v2);

  if (v1 instanceof Value) {
    invariant(v2 instanceof Value);
    return v1.less(v2);
  }

  if (typeof v1 === 'boolean') {
    // $FlowIssue: Flow does not realize that v1 and v2 have the same type.
    return compareBools(v1, v2) === -1;
  }

  invariant(typeof v1 === 'number' || typeof v1 === 'string');
  // $FlowIssue: Flow does not realize that v1 and v2 have the same type.
  return v1 < v2;
}

export function equals(v1: valueOrPrimitive, v2: valueOrPrimitive): boolean {
  if (v1 === v2) {
    return true;
  }

  return v1 instanceof Value && v2 instanceof Value && v1.equals(v2);
}

export function compare(v1: valueOrPrimitive, v2: valueOrPrimitive): number {
  if (equals(v1, v2)) {
    return 0;
  }

  return less(v1, v2) ? -1 : 1;
}

function compareNumbers(v1: number, v2: number) {
  return v1 - v2;
}

function compareObjects(v1: Value, v2: Value) {
  if (v1 === v2 || v1.equals(v2)) {
    return 0;
  }

  return v1.less(v2) ? -1 : 1;
}

function compareStrings(v1: string, v2: string): number {
  if (v1 === v2) {
    return 0;
  }
  return v1 < v2 ? -1 : 1;
}

function compareBools(v1: boolean, v2: boolean): number {
  if (v1 === v2) {
    return 0;
  }
  return getRef(v1).less(getRef(v2)) ? -1 : 1;
}

/**
 * Returns a compare function that can be used with `Array.prototype.sort` based on the type.
 */
export function getCompareFunction(t: Type): (v1: any, v2: any) => number {
  switch (t.kind) {
    case Kind.Number:
      return compareNumbers;

    case Kind.String:
      return compareStrings;

    case Kind.Blob:
    case Kind.List:
    case Kind.Map:
    case Kind.Ref:
    case Kind.Set:
    case Kind.Struct:
    case Kind.Type:
      return compareObjects;

    case Kind.Bool:
      return compareBools;

    case Kind.Value:
      throw new Error('not implemented');

    default:
      invariant(false, 'unreachable');
  }
}
