// @flow

import RefValue from './ref-value.js';
import type {Sequence} from './sequence.js'; // eslint-disable-line no-unused-vars
import type {Type} from './type.js';
import {Value} from './value.js';

export class Collection<S: Sequence> extends Value {
  _type: Type;
  _sequence: S;

  constructor(type: Type, sequence: S) {
    super();
    this._type = type;
    this._sequence = sequence;
  }

  get type(): Type {
    return this._type;
  }

  get sequence(): S {
    return this._sequence;
  }

  isEmpty(): boolean {
    return !this._sequence.isMeta && this._sequence.items.length === 0;
  }

  get chunks(): Array<RefValue> {
    return this._sequence.chunks;
  }
}
