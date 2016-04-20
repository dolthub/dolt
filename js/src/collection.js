// @flow

import RefValue from './ref-value.js';
import type {Sequence} from './sequence.js'; // eslint-disable-line no-unused-vars
import {isPrimitive} from './primitives.js';
import type {MetaTuple} from './meta-sequence.js';
import type {Type} from './type.js';
import {ValueBase} from './value.js';

export class Collection<S: Sequence> extends ValueBase {
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
    const chunks = [];
    const addChunks = this._sequence.isMeta ? (mt: MetaTuple) => {
      chunks.push(new RefValue(mt.ref, this.type));
    } : (v) => {
      if (!isPrimitive(v)) {
        chunks.push(...v.chunks);
      }
    };

    this._sequence.items.forEach(addChunks);
    return chunks;
  }
}
