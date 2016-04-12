// @flow

import type DataStore from './data-store.js';
import type Ref from './ref.js';
import type {Type} from './type.js';
import type {Value, valueOrPrimitive} from './value.js'; // eslint-disable-line no-unused-vars
import {invariant} from './assert.js';
import {refOfValueType} from './type.js';
import {ValueBase} from './value.js';

export default class RefValue<T: valueOrPrimitive> extends ValueBase {
  _type: Type;
  targetRef: Ref;

  constructor(targetRef: Ref, t: Type = refOfValueType) {
    super();
    this._type = t;
    this.targetRef = targetRef;
  }

  get type(): Type {
    return this._type;
  }

  targetValue(store: DataStore): Promise<T> {
    return store.readValue(this.targetRef);
  }

  less(other: Value): boolean {
    invariant(other instanceof RefValue);
    return this.targetRef.less(other.targetRef);
  }

  get chunks(): Array<RefValue> {
    return [this];
  }
}
