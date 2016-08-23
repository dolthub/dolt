// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import Ref from './ref.js';
import type Sequence from './sequence.js'; // eslint-disable-line no-unused-vars
import type {Type} from './type.js';
import {ValueBase} from './value.js';
import {invariant} from './assert.js';
import {init as initValueBase} from './value.js';

export default class Collection<S: Sequence<any>> extends ValueBase {
  sequence: S;

  constructor(sequence: S) {
    super();
    this.sequence = sequence;
  }

  get type(): Type<any> {
    return this.sequence.type;
  }

  isEmpty(): boolean {
    return !this.sequence.isMeta && this.sequence.items.length === 0;
  }

  get chunks(): Array<Ref<any>> {
    return this.sequence.chunks;
  }

  /**
   * Creates a new Collection from a sequence.
   */
  static fromSequence<T: Collection<any>, S: Sequence<any>>(s: S): T {
    const col = Object.create(this.prototype);
    invariant(col instanceof this);
    initValueBase(col);
    col.sequence = s;
    return col;
  }
}
