// @flow

import {Kind, kindToString} from './noms-kind.js';
import {CompoundDesc, getTypeOfValue} from './type.js';
import type {Type} from './type.js';
import {Value} from './value.js';
import {invariant} from './assert.js';
import {equals} from './compare.js';

export default function validateType(t: Type, v: any): void {
  switch (t.kind) {
    case Kind.Bool:
      assertTypeof(v, 'boolean', t);
      return;

    case Kind.Number:
      assertTypeof(v, 'number', t);
      // TODO: Validate value.
      return;

    case Kind.String:
      assertTypeof(v, 'string', t);
      return;

    case Kind.Blob:
    case Kind.List:
    case Kind.Map:
    case Kind.Ref:
    case Kind.Set:
    case Kind.Struct:
    case Kind.Type:
      assertSubtype(v, t);
      return;

    case Kind.Value:
    case Kind.Union:
      assert(subtype(t, getTypeOfValue(v)), v, t);
      break;

    case Kind.Parent:
    default:
      throw new Error('unreachable');
  }
}

function assertSubtype(v: any, t: Type) {
  assert(v instanceof Value, v, t);
  assert(subtype(t, v.type), v, t);
}

function subtype(expected: Type, actual: Type): boolean {
  if (equals(expected, actual)) {
    return true;
  }

  if (expected.kind === Kind.Union) {
    const {desc} = expected;
    invariant(desc instanceof CompoundDesc);
    return desc.elemTypes.some(t => subtype(t, actual));
  }

  if (expected.kind !== actual.kind) {
    return expected.kind === Kind.Value;
  }

  if (expected.desc instanceof CompoundDesc) {
    const actualElemTypes = actual.desc.elemTypes;
    return expected.desc.elemTypes.every((t, i) => compoundSubtype(t, actualElemTypes[i]));
  }

  invariant(false);
}

function compoundSubtype(expected: Type, actual: Type): boolean {
  // In a compound type it is OK to have an empty union.
  if (actual.kind === Kind.Union && actual.desc.elemTypes.length === 0) {
    return true;
  }
  return subtype(expected, actual);
}

function makeTypeError(v: any, t: Type) {
  return new TypeError(`${v} is not a valid ${kindToString(t.kind)}`);
}

function assert(b, v, t) {
  if (!b) {
    throw makeTypeError(v, t);
  }
}

function assertTypeof(v: any, s: string, t: Type) {
  assert(typeof v === s, v, t);
}
