// @flow

import type Database from './database.js';
import {describeType} from './encode-human-readable.js';
import {getRefOfValue} from './get-ref.js';
import {Kind} from './noms-kind.js';
import type Ref from './ref.js';
import type {Type} from './type.js';
import type {valueOrPrimitive} from './value.js'; // eslint-disable-line no-unused-vars
import {invariant} from './assert.js';
import {getTypeOfValue, makeRefType, refOfValueType} from './type.js';
import {Value} from './value.js';

export function refValueFromValue(val: valueOrPrimitive): RefValue {
  return new RefValue(getRefOfValue(val), makeRefType(getTypeOfValue(val)));
}

export default class RefValue<T: valueOrPrimitive> extends Value {
  _type: Type;
  targetRef: Ref;

  constructor(targetRef: Ref, t: Type = refOfValueType) {
    super();
    invariant(t.kind === Kind.Ref, `Not a Ref type: ${describeType(t)}`);
    this._type = t;
    this.targetRef = targetRef;
  }

  get type(): Type {
    return this._type;
  }

  targetValue(db: Database): Promise<T> {
    return db.readValue(this.targetRef);
  }

  less(other: Value): boolean {
    invariant(other instanceof RefValue);
    return this.targetRef.less(other.targetRef);
  }

  get chunks(): Array<RefValue> {
    return [this];
  }
}
