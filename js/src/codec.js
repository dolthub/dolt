// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import Chunk from './chunk.js';
import Hash from './hash.js';
import ValueDecoder from './value-decoder.js';
import ValueEncoder from './value-encoder.js';
import {setEncodeValue} from './get-hash.js';
import {setHash, ValueBase} from './value.js';
import type Value from './value.js';
import type {ValueReader, ValueWriter} from './value-store.js';
import BinaryNomsReader from './binary-noms-reader.js';
import BinaryNomsWriter from './binary-noms-writer.js';

export function encodeValue(v: Value, vw: ?ValueWriter): Chunk {
  const w = new BinaryNomsWriter();
  const enc = new ValueEncoder(w, vw);
  enc.writeValue(v);
  const chunk = new Chunk(w.data);
  if (v instanceof ValueBase) {
    setHash(v, chunk.hash);
  }

  return chunk;
}

setEncodeValue(encodeValue);

export function decodeValue(chunk: Chunk, vr: ValueReader): Value {
  const data = chunk.data;
  const dec = new ValueDecoder(new BinaryNomsReader(data), vr);
  const v = dec.readValue();

  if (v instanceof ValueBase) {
    setHash(v, chunk.hash);
  }

  return v;
}

export interface NomsReader {
  readBytes(): Uint8Array;
  readUint8(): number;
  readUint32(): number;
  readUint64(): number;
  readFloat64(): number;
  readBool(): boolean;
  readString(): string;
  readHash(): Hash;
}

export interface NomsWriter {
  writeBytes(v: Uint8Array): void;
  writeUint8(v: number): void;
  writeUint32(v: number): void;
  writeUint64(v: number): void;
  writeFloat64(v: number): void;
  writeBool(v:boolean): void;
  writeString(v: string): void;
  writeHash(h: Hash): void;
}
