// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {invariant, notNull} from './assert.js';
import Struct, {newStruct} from './struct.js';
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
  valueType,
} from './type.js';
import {equals} from './compare.js';
import {Kind} from './noms-kind.js';
import type {
  CompoundDesc,
  StructDesc,
  Type,
} from './type.js';
import {isSubtype} from './assert-type.js';

const metaIndex = 0;
const parentsIndex = 1;
const valueIndex = 2;

export default class Commit<T: Value> extends Struct {
  constructor(value: T, parents: Set<Ref<Commit>> = new Set(), meta: Struct = getEmptyStruct()) {
    const t = makeCommitType(getTypeOfValue(value), valueTypesFromParents(parents));
    super(t, [meta, parents, value]);
  }

  get value(): T {
    invariant(this.type.desc.fields[valueIndex].name === 'value');
    // $FlowIssue: _values is private.
    const value: T = this._values[valueIndex];
    return value;
  }

  setValue<U: Value>(value: U): Commit<U> {
    return new Commit(value, this.parents);
  }

  get parents(): Set<Ref<Commit<*>>> {
    invariant(this.type.desc.fields[parentsIndex].name === 'parents');
    // $FlowIssue: _values is private.
    const parents: Set<Ref<Commit>> = this._values[parentsIndex];
    invariant(parents instanceof Set);
    return parents;
  }

  setParents(parents: Set<Ref<Commit<*>>>): Commit<T> {
    return new Commit(this.value, parents);
  }

  get meta(): Struct {
    invariant(this.type.desc.fields[metaIndex].name === 'meta');
    // $FlowIssue: _values is private.
    const meta: Struct = this._values[metaIndex];
    invariant(meta instanceof Struct);
    return meta;
  }

  setMeta(meta: Struct): Commit<T> {
    return new Commit(this.value, this.parents, meta);
  }
}

function makeCommitType(valueType: Type<*>, parentsValueTypes: Type<*>[]): Type<StructDesc> {
  const fieldNames = ['meta', 'parents', 'value'];
  const tmp = parentsValueTypes.concat(valueType);
  const parentsValueUnionType = makeUnionType(tmp);
  let parentsType;
  if (equals(parentsValueUnionType, valueType)) {
    parentsType = makeSetType(makeRefType(makeCycleType(0)));
  } else {
    parentsType = makeSetType(makeRefType(makeStructType('Commit', fieldNames, [
      getEmptyStruct().type,
      makeSetType(makeRefType(makeCycleType(0))),
      parentsValueUnionType,
    ])));
  }
  return makeStructType('Commit', fieldNames, [
    getEmptyStruct().type,
    parentsType,
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

// Work around npm cyclic dependencies.
let valueCommitType;
function getValueCommitType() {
  if (!valueCommitType) {
    valueCommitType = makeCommitType(valueType, []);
  }
  return valueCommitType;
}

let emptyStruct;
function getEmptyStruct() {
  if (!emptyStruct) {
    emptyStruct = newStruct('', {});
  }
  return emptyStruct;
}

export function isCommitType(t: Type<StructDesc>): boolean {
  return isSubtype(getValueCommitType(), t);
}
