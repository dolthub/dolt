// @flow

import Ref from './ref.js';
import type Sequence from './sequence.js'; // eslint-disable-line no-unused-vars
import type {Type} from './type.js';
import {ValueBase} from './value.js';

export class Collection<S: Sequence> extends ValueBase {
  sequence: S;

  constructor(sequence: S) {
    super();
    this.sequence = sequence;
  }

  get type(): Type {
    return this.sequence.type;
  }

  isEmpty(): boolean {
    return !this.sequence.isMeta && this.sequence.items.length === 0;
  }

  get chunks(): Array<Ref> {
    return this.sequence.chunks;
  }
}
