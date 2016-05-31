// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import Database from './database.js';
import {makeTestingBatchStore} from './batch-store-adaptor.js';
import type Value from './value.js';
import type Ref from './ref.js';

export default class TestDatabase extends Database {
  writeCount: number;
  constructor() {
    super(makeTestingBatchStore());
    this.writeCount = 0;
  }
  writeValue<T: Value>(v: T): Ref<T> {
    this.writeCount++;
    return super.writeValue(v);
  }
}
