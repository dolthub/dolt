// @flow

import type {ValueReader} from './value-store.js';
import {describeType} from './encode-human-readable.js';
import {getRefOfValue} from './get-ref.js';
import {Kind} from './noms-kind.js';
import type Ref from './ref.js';
import type {Type} from './type.js';
import type {valueOrPrimitive} from './value.js'; // eslint-disable-line no-unused-vars
import {invariant} from './assert.js';
import {getTypeOfValue, makeRefType} from './type.js';
import {Value, getChunksOfValue} from './value.js';

export function constructRefValue(t: Type, targetRef: Ref, height: number): RefValue {
  invariant(t.kind === Kind.Ref, () => `Not a Ref type: ${describeType(t)}`);
  invariant(!targetRef.isEmpty());
  invariant(height > 0);
  const rv = Object.create(RefValue.prototype);
  rv._type = t;
  rv.targetRef = targetRef;
  rv.height = height;
  return rv;
}

export default class RefValue<T: valueOrPrimitive> extends Value {
  _type: Type;
  // Ref of the value this points to.
  targetRef: Ref;
  // The length of the longest path of RefValues to find any leaf in the graph.
  // By definition this must be > 0.
  height: number;

  constructor(val: T) {
    super();
    this._type = makeRefType(getTypeOfValue(val));
    this.height = 1 + getChunksOfValue(val).reduce((max, c) => Math.max(max, c.height), 0);
    this.targetRef = getRefOfValue(val);
  }

  get type(): Type {
    return this._type;
  }

  targetValue(vr: ValueReader): Promise<T> {
    return vr.readValue(this.targetRef);
  }

  less(other: Value): boolean {
    invariant(other instanceof RefValue);
    return this.targetRef.less(other.targetRef);
  }

  get chunks(): Array<RefValue> {
    return [this];
  }
}
