// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {Kind, kindToString} from './noms-kind.js';
import {CompoundDesc, getTypeOfValue} from './type.js';
import type {Type} from './type.js';
import {invariant} from './assert.js';
import {equals} from './compare.js';
import type Value from './value.js';

/**
 * Ensures that the Noms value is a subtype of the Noms type. Throws a `TypeError` if not.
 * Here are the rules to determine if a value is a subtype: `<` is used to symbolize subtype.
 * ```
 * true < Bool
 * 4 < Number
 * "hi" < String
 * true < Bool | Number
 * [1, 2] < List<Number>
 * new Set(["hi"]) < Set<String>
 * new Map([1, "one"]) < Set<Number, String>
 * [1, "hi"] < List<Number | String>
 *
 * [] < List<>
 * [] < List<T> for all T
 * new Set() < Set<T> for all T
 * new Map() < Map<T, V> for all T and V
 *
 * newStruct("S", {x: 42}) < struct S {x: Number}
 * newStruct("S", {x: 42}) < struct "" {x: Number}, non nominal struct
 * newStruct("", {x: 42}) </ struct S {x: Number}, not a subtype
 * newStruct("S", {x: 42, y: true}) < struct S {x: Number}, extra fields OK.
 * ```
 */
export default function assertSubtype(requiredType: Type, v: Value): void {
  assert(isSubtype(requiredType, getTypeOfValue(v)), v, requiredType);
}

export function isSubtype(requiredType: Type, concreteType: Type): boolean {
  if (equals(requiredType, concreteType)) {
    return true;
  }

  if (requiredType.kind === Kind.Union) {
    const {desc} = requiredType;
    invariant(desc instanceof CompoundDesc);
    return desc.elemTypes.some(t => isSubtype(t, concreteType));
  }

  if (requiredType.kind !== concreteType.kind) {
    return requiredType.kind === Kind.Value;
  }

  const requiredDesc = requiredType.desc;
  const concreteDesc = concreteType.desc;
  if (requiredDesc instanceof CompoundDesc) {
    const concreteTypeElemTypes = concreteDesc.elemTypes;
    return requiredDesc.elemTypes.every((t, i) => compoundSubtype(t, concreteTypeElemTypes[i]));
  }

  if (requiredType.kind === Kind.Struct) {
    if (requiredDesc.name !== '' && requiredDesc.name !== concreteDesc.name) {
      return false;
    }

    let j = 0;
    const requiredFields = requiredDesc.fields;
    const concreteFields = concreteDesc.fields;
    for (let i = 0; i < requiredFields.length; i++) {
      const name = requiredFields[i].name;
      for (; j < concreteFields.length && concreteFields[j].name !== name; j++);
      if (j === concreteFields.length) {
        return false;
      }

      if (!isSubtype(requiredFields[i].type, concreteFields[j].type)) {
        return false;
      }
    }
    return true;
  }

  invariant(false);
}

function compoundSubtype(requiredType: Type, concreteType: Type): boolean {
  // In a compound type it is OK to have an empty union.
  if (concreteType.kind === Kind.Union && concreteType.desc.elemTypes.length === 0) {
    return true;
  }
  return isSubtype(requiredType, concreteType);
}

function assert(b, v, t) {
  if (!b) {
    throw new TypeError(`${v} is not a valid ${kindToString(t.kind)}`);
  }
}
