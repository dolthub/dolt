// @flow

import {invariant} from './assert.js';
import {getDatasTypes} from './database.js';
import Struct from './struct.js';
import type Value from './value.js';
import type RefValue from './ref-value.js';
import Set from './set.js';


export default class Commit<T: Value> extends Struct {
  constructor(value: T, parents: Set<RefValue<Commit>> = new Set()) {
    const {commitType} = getDatasTypes();
    super(commitType, {value, parents});
  }

  get value(): T {
    // $FlowIssue: _data is private.
    const value: T = this._data.value;
    return value;
  }

  setValue<U: Value>(value: U): Commit<U> {
    return new Commit(value, this.parents);
  }

  get parents(): Set<RefValue<Commit>> {
    // $FlowIssue: _data is private.
    const parents: Set<RefValue<Commit>> = this._data.parents;
    invariant(parents instanceof Set);
    return parents;
  }

  setParents(parents: Set<RefValue<Commit>>): Commit<T> {
    return new Commit(this.value, parents);
  }
}
