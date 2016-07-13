// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {invariant} from './assert.js';
import Struct from './struct.js';
import type Value from './value.js';
import type Ref from './ref.js';
import Set from './set.js';
import {
  makeCycleType,
  makeRefType,
  makeStructType,
  makeSetType,
  valueType,
} from './type.js';

export const commitType = makeStructType('Commit',
  ['parents', 'value'],
  [
    makeSetType(makeRefType(makeCycleType(0))),
    valueType,
  ]
);

export default class Commit<T: Value> extends Struct {
  constructor(value: T, parents: Set<Ref<Commit>> = new Set()) {
    super(commitType, [parents, value]);
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
