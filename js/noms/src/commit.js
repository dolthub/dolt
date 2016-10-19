// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

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
  constructor(value: T, parents: Set<Ref<Commit<any>>> = new Set(),
              meta: Struct = getEmptyStruct()) {
    const t = makeCommitType(getTypeOfValue(value), valueTypesFromParents(parents, 'value'),
                             getTypeOfValue(meta), valueTypesFromParents(parents, 'meta'));
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

  get parents(): Set<Ref<Commit<any>>> {
    invariant(this.type.desc.fields[parentsIndex].name === 'parents');
    // $FlowIssue: _values is private.
    const parents: Set<Ref<Commit>> = this._values[parentsIndex];
    invariant(parents instanceof Set);
    return parents;
  }

  setParents(parents: Set<Ref<Commit<any>>>): Commit<T> {
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

// ../../go/datas/commit.go for the motivation for how this is computed.
function makeCommitType(valueType: Type<any>, parentsValueTypes: Type<any>[],
                        metaType: Type<any>, parentsMetaTypes: Type<any>[]): Type<StructDesc> {
  const parentsValueUnionType = makeUnionType(parentsValueTypes.concat(valueType));
  const parentsMetaUnionType = makeUnionType(parentsMetaTypes.concat(metaType));
  let parentsType;
  if (equals(parentsValueUnionType, valueType) && equals(parentsMetaUnionType, metaType)) {
    parentsType = makeSetType(makeRefType(makeCycleType(0)));
  } else {
    parentsType = makeSetType(makeRefType(makeStructType('Commit', {
      meta: parentsMetaUnionType,
      parents: makeSetType(makeRefType(makeCycleType(0))),
      value: parentsValueUnionType,
    })));
  }
  return makeStructType('Commit', {
    meta: metaType,
    parents: parentsType,
    value: valueType,
  });
}

function valueTypesFromParents(parents: Set<any>, fieldName: string): Type<any>[] {
  const elemType = getSetElementType(parents.type);
  switch (elemType.kind) {
    case Kind.Union:
      return elemType.desc.elemTypes.map(t => fieldTypeFromRefOfCommit(t, fieldName));
    default:
      return [fieldTypeFromRefOfCommit(elemType, fieldName)];
  }
}

function getSetElementType(t: Type<CompoundDesc>): Type<any> {
  invariant(t.kind === Kind.Set);
  return t.desc.elemTypes[0];
}

function fieldTypeFromRefOfCommit(t: Type<CompoundDesc>, fieldName: string): Type<any> {
  return fieldTypeFromCommit(getRefElementType(t), fieldName);
}

function getRefElementType(t: Type<CompoundDesc>): Type<any> {
  invariant(t.kind === Kind.Ref);
  return t.desc.elemTypes[0];
}

function fieldTypeFromCommit(t: Type<StructDesc>, fieldName: string): Type<any> {
  invariant(t.desc.name === 'Commit');
  return notNull(t.desc.getField(fieldName));
}

// Work around npm cyclic dependencies.
let valueCommitType;
function getValueCommitType() {
  if (!valueCommitType) {
    valueCommitType = makeCommitType(valueType, [], getEmptyStruct().type, []);
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
