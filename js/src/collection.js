  // @flow

import Ref from './ref.js';
import type {ChunkStore} from './chunk_store.js';
import type {Sequence} from './sequence.js'; // eslint-disable-line no-unused-vars
import {isPrimitive} from './primitives.js';
import {MetaTuple} from './meta_sequence.js';
import {Type} from './type.js';
import {ValueBase} from './value.js';

export class Collection<S:Sequence> extends ValueBase {
  sequence: S;
  cs: ChunkStore;

  constructor(cs: ChunkStore, type: Type, sequence: S) {
    super(type);
    this.cs = cs;
    this.sequence = sequence;
  }

  isEmpty(): boolean {
    return !this.sequence.isMeta && this.sequence.items.length === 0;
  }

  get chunks(): Array<Ref> {
    const chunks = [];
    const addChunks = this.sequence.isMeta ? (mt:MetaTuple) => {
      chunks.push(mt.ref);
    } : (v) => {
      if (!isPrimitive(v)) {
        chunks.push(...v.chunks);
      }
    };

    this.sequence.items.forEach(addChunks);
    return chunks;
  }
}
