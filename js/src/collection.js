  // @flow

import type {ChunkStore} from './chunk_store.js';
import {Type} from './type.js';
import {ValueBase} from './value.js';
import type {Sequence} from './sequence.js'; // eslint-disable-line no-unused-vars

export class Collection<S:Sequence> extends ValueBase {
  sequence: S;
  cs: ChunkStore;

  constructor(cs: ChunkStore, type: Type, sequence: S) {
    super(type);
    this.cs = cs;
    this.sequence = sequence;
  }
}
