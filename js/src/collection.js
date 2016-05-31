// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import Ref from './ref.js';
import type Sequence from './sequence.js'; // eslint-disable-line no-unused-vars
import type {Type} from './type.js';
import {ValueBase} from './value.js';
import {invariant} from './assert.js';
import {init as initValueBase} from './value.js';

export default class Collection<S: Sequence> extends ValueBase {
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

  /**
   * Creates a new Collection from a sequence.
   */
  static fromSequence<T: Collection, S: Sequence>(s: S): T {
    const col = Object.create(this.prototype);
    invariant(col instanceof this);
    initValueBase(col);
    col.sequence = s;
    return col;
  }
}
