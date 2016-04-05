// @flow

import {Kind, kindToString} from './noms-kind.js';
import {CompoundDesc} from './type.js';
import type {Type} from './type.js';
import {ValueBase} from './value.js';
import {invariant} from './assert.js';

export default function validateType(t: Type, v: any): void {
  switch (t.kind) {
    case Kind.Bool:
      assertTypeof(v, 'boolean', t);
      return;

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
      assertTypeof(v, 'number', t);
      // TODO: Validate value.
      return;

    case Kind.String:
      assertTypeof(v, 'string', t);
      return;

    case Kind.Value: {
      const s = typeof v;
      assert(v instanceof ValueBase || s === 'boolean' || s === 'number' || s === 'string', v, t);
      return;
    }

    case Kind.Enum:
      assertTypeof(v, 'number', t);
      // TODO: Validate value.
      return;

    case Kind.List:
    case Kind.Map:
    case Kind.Ref:
    case Kind.Set:
    case Kind.Blob:
    case Kind.Struct:
    case Kind.Type:
    case Kind.Package:
    case Kind.Unresolved:
      assertSubtype(v, t);
      return;

    default:
      throw new Error('unreachable');
  }
}

function assertSubtype(v: any, t: Type) {
  assert(v instanceof ValueBase, v, t);
  assert(subtype(t, v.type), v, t);
}

function subtype(expected: Type, actual: Type): boolean {
  if (expected.equals(actual)) {
    return true;
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
