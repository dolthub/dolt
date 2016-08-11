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
export default function assertSubtype(requiredType: Type<any>, v: Value): void {
  assert(isSubtype(requiredType, getTypeOfValue(v)), v, requiredType);
}

export function isSubtype(requiredType: Type<any>, concreteType: Type<any>): boolean {
  return isSubtypeInternal(requiredType, concreteType, []);
}

export function isSubtypeInternal(requiredType: Type<any>, concreteType: Type<any>,
                                  parentStructTypes: Type<any>[]): boolean {
  if (equals(requiredType, concreteType)) {
    return true;
  }

  if (requiredType.kind === Kind.Union) {

    // If we're comparing two unions all component types must be compatible
    if (concreteType.kind === Kind.Union) {
      const {desc} = concreteType;
      invariant(desc instanceof CompoundDesc);
      return desc.elemTypes.every(t => isSubtypeInternal(requiredType, t, parentStructTypes));
    }
    const {desc} = requiredType;
    invariant(desc instanceof CompoundDesc);
    return desc.elemTypes.some(t => isSubtypeInternal(t, concreteType, parentStructTypes));
  }

  if (requiredType.kind !== concreteType.kind) {
    return requiredType.kind === Kind.Value;
  }

  const requiredDesc = requiredType.desc;
  const concreteDesc = concreteType.desc;
  if (requiredDesc instanceof CompoundDesc) {
    const concreteTypeElemTypes = concreteDesc.elemTypes;
    return requiredDesc.elemTypes.every(
      (t, i) => compoundSubtype(t, concreteTypeElemTypes[i], parentStructTypes));
  }

  if (requiredType.kind === Kind.Struct) {
    if (requiredDesc.name !== '' && requiredDesc.name !== concreteDesc.name) {
      return false;
    }

    // We may already be computing the subtype for this type if we have a cycle. In that case we
    // exit the recursive check. We may still find that the type is not a subtype but that will be
    // handled at a higher level in the callstack.
    const idx = parentStructTypes.indexOf(requiredType);
    if (idx !== -1) {
      return true;
    }

    let j = 0;
    const requiredFields = requiredDesc.fields;
    const concreteFields = concreteDesc.fields;
    for (let i = 0; i < requiredFields.length; i++) {
      const requiredField = requiredFields[i];
      const {name} = requiredField;
      for (; j < concreteFields.length && concreteFields[j].name !== name; j++);
      if (j === concreteFields.length) {
        return false;
      }

      parentStructTypes.push(requiredType);
      const b = isSubtypeInternal(requiredField.type, concreteFields[j].type, parentStructTypes);
      parentStructTypes.pop();
      if (!b) {
        return false;
      }
    }
    return true;
  }

  invariant(false);
}

function compoundSubtype(requiredType: Type<any>, concreteType: Type<any>,
                         parentStructTypes: Type<any>[]): boolean {
  // In a compound type it is OK to have an empty union.
  if (concreteType.kind === Kind.Union && concreteType.desc.elemTypes.length === 0) {
    return true;
  }
  return isSubtypeInternal(requiredType, concreteType, parentStructTypes);
}

function assert(b, v, t) {
  if (!b) {
    throw new TypeError(`${String(v)} is not a valid ${kindToString(t.kind)}`);
  }
}
