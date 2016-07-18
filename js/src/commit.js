// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {invariant, notNull} from './assert.js';
import Struct from './struct.js';
import type Value from './value.js';
import type Ref from './ref.js';
import Set from './set.js';
import {
  getTypeOfValue,
  makeCycleType,
  makeRefType,
  makeSetType,
  makeStructType,
  makeUnionType,
} from './type.js';
import {equals} from './compare.js';
import {Kind} from './noms-kind.js';
import type {
  CompoundDesc,
  StructDesc,
  Type,
} from './type.js';

export default class Commit<T: Value> extends Struct {
  constructor(value: T, parents: Set<Ref<Commit>> = new Set()) {
    const t = makeCommitType(getTypeOfValue(value), valueTypesFromParents(parents));
    super(t, [parents, value]);
  }

  get value(): T {
    invariant(this.type.desc.fields[1].name === 'value');
    // $FlowIssue: _values is private.
    const value: T = this._values[1];
    return value;
  }

  setValue<U: Value>(value: U): Commit<U> {
    return new Commit(value, this.parents);
  }

  get parents(): Set<Ref<Commit>> {
    invariant(this.type.desc.fields[0].name === 'parents');
    // $FlowIssue: _values is private.
    const parents: Set<Ref<Commit>> = this._values[0];
    invariant(parents instanceof Set);
    return parents;
  }

  setParents(parents: Set<Ref<Commit>>): Commit<T> {
    return new Commit(this.value, parents);
  }
}

function makeCommitType(valueType: Type<*>, parentsValueTypes: Type<*>[]): Type<StructDesc> {
  const tmp = parentsValueTypes.concat(valueType);
  const parentsValueUnionType = makeUnionType(tmp);
  if (equals(parentsValueUnionType, valueType)) {
    return makeStructType('Commit', [
      'parents', 'value',
    ], [
      makeSetType(makeRefType(makeCycleType(0))),
      valueType,
    ]);
  }
  return makeStructType('Commit', ['parents', 'value'], [
    makeSetType(makeRefType(makeStructType('Commit', [
      'parents', 'value',
    ], [
      makeSetType(makeRefType(makeCycleType(0))),
      parentsValueUnionType,
    ]))),
    valueType,
  ]);
}

function valueTypesFromParents(parents: Set): Type<*>[] {
  const elemType = getSetElementType(parents.type);
  switch (elemType.kind) {
    case Kind.Union:
      return elemType.desc.elemTypes.map(valueFromRefOfCommit);
    default:
      return [valueFromRefOfCommit(elemType)];
  }
}

function getSetElementType(t: Type<CompoundDesc>): Type<*> {
  invariant(t.kind === Kind.Set);
  return t.desc.elemTypes[0];
}

function valueFromRefOfCommit(t: Type<CompoundDesc>): Type<*> {
  return valueTypeFromCommit(getRefElementType(t));
}

function getRefElementType(t: Type<CompoundDesc>): Type<*> {
  invariant(t.kind === Kind.Ref);
  return t.desc.elemTypes[0];
}

function valueTypeFromCommit(t: Type<StructDesc>): Type<*> {
  invariant(t.name === 'Commit');
  return notNull(t.desc.getField('value'));
}
