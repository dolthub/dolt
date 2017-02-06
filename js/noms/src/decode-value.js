// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import type Chunk from './chunk.js';
import ValueDecoder from './value-decoder.js';
import type Value from './value.js';
import type {ValueReader} from './value-store.js';
import {staticTypeCache} from './type-cache.js';
import {setHash, ValueBase} from './value.js';
import BinaryNomsReader from './binary-noms-reader.js';

export default function decodeValue(chunk: Chunk, vr: ValueReader): Value {
  const data = chunk.data;
  const br = new BinaryNomsReader(data);
  const dec = new ValueDecoder(br, vr, staticTypeCache);
  const v = dec.readValue();
  if (br.pos() !== data.byteLength) {
    throw new Error('Invalid chunk data: not all bytes consumed');
  }
  if (v instanceof ValueBase) {
    setHash(v, chunk.hash);
  }

  return v;
}
