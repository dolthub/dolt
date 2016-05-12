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

    case Kind.Value: {
      const s = typeof v;
      assert(v instanceof Value || s === 'boolean' || s === 'number' || s === 'string', v, t);
      return;
    }

    case Kind.Blob:
    case Kind.List:
    case Kind.Map:
    case Kind.Ref:
    case Kind.Set:
    case Kind.Struct:
    case Kind.Type:
      assertSubtype(v, t);
      return;

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
    const {desc} = actual;
    invariant(desc instanceof CompoundDesc);
    return expected.desc.elemTypes.every((t, i) => subtype(t, desc.elemTypes[i]));
  }

  invariant(false);
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
