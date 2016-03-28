// @flow

import type {valueOrPrimitive, Value} from './value.js';
import {invariant} from './assert.js';
import {Kind} from './noms-kind.js';
import type {Type} from './type.js';

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

function compareNumbers(v1: number, v2: number) {
  return v1 - v2;
}

function compareObjects(v1: Value, v2: Value) {
  return v1.less(v2) ? -1 : 1;
}

function compareStrings(v1: string, v2: string): number {
  return v1 < v2 ? -1 : 1;
}

/**
 * Returns a compare function that can be used with `Array.prototype.sort` based on the type.
 */
export function getCompareFunction(t: Type): (v1: any, v2: any) => number {
  switch (t.kind) {
    case Kind.Uint8:
    case Kind.Uint16:
    case Kind.Uint32:
    case Kind.Uint64:
    case Kind.Int8:
    case Kind.Int16:
    case Kind.Int32:
    case Kind.Int64:
    case Kind.Float32:
    case Kind.Float64:
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
    case Kind.Unresolved:
    case Kind.Package:
      return compareObjects;

    case Kind.Value:
    case Kind.Bool:
      throw new Error('not implemented');

    default:
      invariant(false, 'unreachable');
  }
}
