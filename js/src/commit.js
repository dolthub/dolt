// @flow

import {invariant} from './assert.js';
import {getDatasTypes} from './database.js';
import Struct from './struct.js';
import type Value from './value.js';
import type Ref from './ref.js';
import Set from './set.js';


export default class Commit<T: Value> extends Struct {
  constructor(value: T, parents: Set<Ref<Commit>> = new Set()) {
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

  get parents(): Set<Ref<Commit>> {
    // $FlowIssue: _data is private.
    const parents: Set<Ref<Commit>> = this._data.parents;
    invariant(parents instanceof Set);
    return parents;
  }

  setParents(parents: Set<Ref<Commit>>): Commit<T> {
    return new Commit(this.value, parents);
  }
}
