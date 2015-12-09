// @flow

'use strict';

import Ref from './ref.js';
import {ensureRef} from './get_ref.js';
import {Type} from './type.js';
import type {ChunkStore} from './chunk_store.js';

export class MetaSequence {
  tuples: Array<MetaTuple>;
  type: Type;
  _ref: ?Ref;
  _cs: ChunkStore;

  constructor(cs: ChunkStore, type: Type, tuples: Array<MetaTuple>) {
    this._cs = cs;
    this.type = type;
    this.tuples = tuples;
    this._ref = null;
  }

  get ref(): Ref {
    return this._ref = ensureRef(this._ref, this, this.type);
  }

}

export class MetaTuple {
  ref: Ref;
  value: any;

  constructor(ref: Ref, value: any) {
    this.ref = ref;
    this.value = value;
  }
}
