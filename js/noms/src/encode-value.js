// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import Chunk from './chunk.js';
import ValueEncoder from './value-encoder.js';
import type Value from './value.js';
import type {ValueWriter} from './value-store.js';
import {setHash, ValueBase} from './value.js';
import BinaryNomsWriter from './binary-noms-writer.js';

export default function encodeValue(v: Value, vw: ?ValueWriter): Chunk {
  const w = new BinaryNomsWriter();
  const enc = new ValueEncoder(w, vw);
  enc.writeValue(v);
  const chunk = new Chunk(w.data);
  if (v instanceof ValueBase) {
    setHash(v, chunk.hash);
  }

  return chunk;
}
