// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import * as Bytes from './bytes.js';
import Hash, {byteLength as hashByteLength} from './hash.js';
import ValueEncoder from './value-encoder.js';
import type {Type} from './type.js';
import {notNull} from './assert.js';
import {BinaryWriter} from './binary-rw.js';

function ensureTypeSerialization(t: Type<any>) {
  if (!t.serialization) {
    const w = new BinaryNomsWriter();
    const enc = new ValueEncoder(w, null);
    enc.writeType(t, []);
    t.serialization = w.data;
  }
}

export default class BinaryNomsWriter extends BinaryWriter {
  constructor() {
    super();
  }

  writeHash(h: Hash): void {
    this.ensureCapacity(hashByteLength);
    Bytes.copy(h.digest, this.buff, this.offset);
    this.offset += hashByteLength;
  }

  appendType(t: Type<any>): void {
    // Note: The JS & Go impls differ here. The Go impl eagerly serializes types as they are
    // constructed. The JS does it lazily so as to avoid cyclic package dependencies.
    ensureTypeSerialization(t);
    const data = notNull(t.serialization);
    const size = data.byteLength;
    this.ensureCapacity(size);

    Bytes.copy(data, this.buff, this.offset);
    this.offset += size;
  }
}
