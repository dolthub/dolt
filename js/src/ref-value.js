// @flow

import DataStore from './data-store.js';
import Ref from './ref.js';
import type {Type} from './type.js';
import type {Value} from './value.js'; // eslint-disable-line no-unused-vars
import {invariant} from './assert.js';
import {refOfValueType} from './type.js';
import {ValueBase} from './value.js';

export default class RefValue<T: Value> extends ValueBase {
  targetRef: Ref;

  constructor(targetRef: Ref, t: Type = refOfValueType) {
    super(t);
    this.targetRef = targetRef;
  }

  targetValue(store: DataStore): Promise<T> {
    return store.readValue(this.targetRef);
  }

  less(other: Value): boolean {
    invariant(other instanceof RefValue);
    return this.targetRef.less(other.targetRef);
  }

  get chunks(): Array<Ref> {
    return [this.targetRef];
  }
}
