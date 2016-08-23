// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import * as Bytes from './bytes.js';
import Chunk from './chunk.js';
import Hash, {byteLength as hashByteLength} from './hash.js';
import ValueDecoder from './value-decoder.js';
import ValueEncoder from './value-encoder.js';
import type Value from './value.js';
import type {Type} from './type.js';
import type {ValueReader, ValueWriter} from './value-store.js';
import {default as TypeCache, staticTypeCache} from './type-cache.js';
import {notNull} from './assert.js';
import {setEncodeValue} from './get-hash.js';
import {setHash, ValueBase} from './value.js';
import {BinaryReader, BinaryWriter} from './binary-rw.js';

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

function ensureTypeSerialization(t: Type<any>) {
  if (!t.serialization) {
    const w = new BinaryNomsWriter();
    const enc = new ValueEncoder(w, null);
    enc.writeType(t, []);
    t.serialization = w.data;
  }
}

export interface NomsReader {
  pos(): number;
  seek(pos: number): void;
  readBytes(): Uint8Array;
  readUint8(): number;
  readUint32(): number;
  readUint64(): number;
  readNumber(): number;
  readBool(): boolean;
  readString(): string;
  readIdent(tc: TypeCache): number;
  readHash(): Hash;
}

export interface NomsWriter {
  writeBytes(v: Uint8Array): void;
  writeUint8(v: number): void;
  writeUint32(v: number): void;
  writeUint64(v: number): void;
  writeNumber(v: number): void;
  writeBool(v:boolean): void;
  writeString(v: string): void;
  writeHash(h: Hash): void;
  appendType(t: Type<any>): void;
}

export class BinaryNomsReader extends BinaryReader {
  constructor(buff: Uint8Array) {
    super(buff);
  }

  readIdent(tc: TypeCache): number {
    const str = this.readString(); // TODO: Figure out how to do this without allocating.
    let id = tc.identTable.entries.get(str);
    if (id === undefined) {
      id = tc.identTable.getId(str);
    }

    return id;
  }

  readHash(): Hash {
    // Make a copy of the data.
    const digest = Bytes.slice(this.buff, this.offset, this.offset + hashByteLength);
    this.offset += hashByteLength;
    return new Hash(digest);
  }
}

export class BinaryNomsWriter extends BinaryWriter {
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
