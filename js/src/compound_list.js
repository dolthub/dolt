// @flow

import type {ChunkStore} from './chunk_store.js';
import {MetaTuple, MetaSequence} from './meta_sequence.js';
import {Type} from './type.js';

export default class CompoundList extends MetaSequence {

  constructor(cs: ChunkStore, type: Type, tuples: Array<MetaTuple>) {
    super(cs, type, tuples);
  }
}
