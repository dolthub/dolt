// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import * as Bytes from './bytes.js';
import Chunk from './chunk.js';
import Hash, {byteLength as hashByteLength} from './hash.js';
import ValueDecoder from './value-decoder.js';
import ValueEncoder from './value-encoder.js';
import svarint from 'signed-varint';
import type Value from './value.js';
import type {Type} from './type.js';
import type {ValueReader, ValueWriter} from './value-store.js';
import {default as TypeCache, staticTypeCache} from './type-cache.js';
import {floatToIntExp, intExpToFloat} from './number-util.js';
import {invariant, notNull} from './assert.js';
import {setEncodeValue} from './get-hash.js';
import {setHash, ValueBase} from './value.js';

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
  const dec = new ValueDecoder(new BinaryNomsReader(data), vr, staticTypeCache);
  const v = dec.readValue();

  if (v instanceof ValueBase) {
    setHash(v, chunk.hash);
  }

  return v;
}

function ensureTypeSerialization(t: Type) {
  if (!t.serialization) {
    const w = new BinaryNomsWriter();
    const enc = new ValueEncoder(w, null);
    enc.writeType(t, []);
    t.serialization = w.data;
  }
}

const maxUInt32 = Math.pow(2, 32);
const bigEndian = false;

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
  appendType(t: Type): void;
}

export class BinaryNomsReader {
  buff: Uint8Array;
  dv: DataView;
  offset: number;

  constructor(buff: Uint8Array) {
    this.buff = buff;
    this.dv = new DataView(buff.buffer, buff.byteOffset, buff.byteLength);
    this.offset = 0;
  }

  pos(): number {
    return this.offset;
  }

  seek(pos: number): void {
    this.offset = pos;
  }

  readBytes(): Uint8Array {
    const size = this.readUint32();
    // Make a copy of the buffer to return
    const v = Bytes.slice(this.buff, this.offset, this.offset + size);
    this.offset += size;
    return v;
  }

  readUint8(): number {
    const v = this.dv.getUint8(this.offset);
    this.offset++;
    return v;
  }

  readUint32(): number {
    const v = this.dv.getUint32(this.offset, bigEndian);
    this.offset += 4;
    return v;
  }

  readUint64(): number {
    // Big endian
    const msi = this.readUint32();
    const lsi = this.readUint32();
    const v = msi * maxUInt32 + lsi;
    invariant(v <= Number.MAX_SAFE_INTEGER);
    return v;
  }

  readNumber(): number {
    const intVal = svarint.decode(this.buff, this.offset);
    this.offset += svarint.decode.bytes;
    const expVal = svarint.decode(this.buff, this.offset);
    this.offset += svarint.decode.bytes;
    return intExpToFloat(intVal, expVal);
  }

  readBool(): boolean {
    const v = this.readUint8();
    invariant(v === 0 || v === 1);
    return v === 1;
  }

  readString(): string {
    const size = this.readUint32();
    const str = Bytes.readUtf8(this.buff, this.offset, this.offset + size);
    this.offset += size;
    return str;
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

const initialBufferSize = 16;

export class BinaryNomsWriter {
  buff: Uint8Array;
  dv: DataView;
  offset: number;

  constructor() {
    this.buff = Bytes.alloc(initialBufferSize);
    this.dv = new DataView(this.buff.buffer, 0);
    this.offset = 0;
  }

  get data(): Uint8Array {
    // Callers now owns the copied data.
    return Bytes.slice(this.buff, 0, this.offset);
  }

  ensureCapacity(n: number): void {
    let length = this.buff.byteLength;
    if (this.offset + n <= length) {
      return;
    }

    while (this.offset + n > length) {
      length *= 2;
    }

    this.buff = Bytes.grow(this.buff, length);
    this.dv = new DataView(this.buff.buffer);
  }

  writeBytes(v: Uint8Array): void {
    const size = v.byteLength;
    this.writeUint32(size);

    this.ensureCapacity(size);
    Bytes.copy(v, this.buff, this.offset);
    this.offset += size;
  }

  writeUint8(v: number): void {
    this.ensureCapacity(1);
    this.dv.setUint8(this.offset, v);
    this.offset++;
  }

  writeUint32(v: number): void {
    this.ensureCapacity(4);
    this.dv.setUint32(this.offset, v, bigEndian);
    this.offset += 4;
  }

  writeUint64(v: number): void {
    invariant(v <= Number.MAX_SAFE_INTEGER);
    const msi = (v / maxUInt32) | 0;
    const lsi = v % maxUInt32;
    // Big endian
    this.writeUint32(msi);
    this.writeUint32(lsi);
  }

  writeNumber(v: number): void {
    const [intVal, expVal] = floatToIntExp(v);
    const intLen = svarint.encodingLength(intVal);
    const expLen = svarint.encodingLength(expVal);
    this.ensureCapacity(intLen + expLen);

    svarint.encode(intVal, this.buff, this.offset);
    this.offset += intLen;
    svarint.encode(expVal, this.buff, this.offset);
    this.offset += expLen;
  }

  writeBool(v:boolean): void {
    this.writeUint8(v ? 1 : 0);
  }

  writeString(v: string): void {
    // TODO: This is a bummer. Ensure even the largest UTF8 string will fit.
    this.ensureCapacity(4 + v.length * 4);
    this.offset = Bytes.encodeUtf8(v, this.buff, this.dv, this.offset);
  }

  writeHash(h: Hash): void {
    this.ensureCapacity(hashByteLength);
    Bytes.copy(h.digest, this.buff, this.offset);
    this.offset += hashByteLength;
  }

  appendType(t: Type): void {
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
