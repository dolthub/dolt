// @flow

import RefValue from './ref-value.js';
import type {Sequence} from './sequence.js'; // eslint-disable-line no-unused-vars
import {isPrimitive} from './primitives.js';
import type {MetaTuple} from './meta-sequence.js';
import type {Type} from './type.js';
import {ValueBase} from './value.js';

export class Collection<S: Sequence> extends ValueBase {
  _type: Type;
  sequence: S;

  constructor(type: Type, sequence: S) {
    super();
    this._type = type;
    this.sequence = sequence;
  }

  get type(): Type {
    return this._type;
  }

  isEmpty(): boolean {
    return !this.sequence.isMeta && this.sequence.items.length === 0;
  }

  get chunks(): Array<RefValue> {
    const chunks = [];
    const addChunks = this.sequence.isMeta ? (mt: MetaTuple) => {
      chunks.push(new RefValue(mt.ref, this.type));
    } : (v) => {
      if (!isPrimitive(v)) {
        chunks.push(...v.chunks);
      }
    };

    this.sequence.items.forEach(addChunks);
    return chunks;
  }
}
