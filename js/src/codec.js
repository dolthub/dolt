// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import Chunk from './chunk.js';
import Hash, {sha1Size} from './hash.js';
import ValueDecoder from './value-decoder.js';
import ValueEncoder from './value-encoder.js';
import {encode, decode} from './utf8.js';
import {invariant} from './assert.js';
import {setEncodeValue} from './get-hash.js';
import {setHash, ValueBase} from './value.js';
import type Value from './value.js';
import type {ValueReader, ValueWriter} from './value-store.js';

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


const maxUInt32 = Math.pow(2, 32);
const littleEndian = true;

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

export class BinaryNomsReader {
  buff: ArrayBuffer;
  dv: DataView;
  offset: number;
  length: number;

  constructor(data: Uint8Array) {
    this.buff = data.buffer;
    this.offset = data.byteOffset;
    this.length = data.byteLength;
    this.dv = new DataView(this.buff, this.offset, this.length);
  }

  readBytes(): Uint8Array {
    const size = this.readUint32();
    // Make a copy of the buffer to return
    const v = new Uint8Array(new Uint8Array(this.buff, this.offset, size));
    this.offset += size;
    return v;
  }

  readUint8(): number {
    const v = this.dv.getUint8(this.offset);
    this.offset++;
    return v;
  }

  readUint32(): number {
    const v = this.dv.getUint32(this.offset, littleEndian);
    this.offset += 4;
    return v;
  }

  readUint64(): number {
    const lsi = this.readUint32();
    const msi = this.readUint32();
    const v = msi * maxUInt32 + lsi;
    invariant(v <= Number.MAX_SAFE_INTEGER);
    return v;
  }

  readFloat64(): number {
    const v = this.dv.getFloat64(this.offset, littleEndian);
    this.offset += 8;
    return v;
  }

  readBool(): boolean {
    const v = this.readUint8();
    invariant(v === 0 || v === 1);
    return v === 1;
  }

  readString(): string {
    const size = this.readUint32();
    const v = new Uint8Array(this.buff, this.offset, size);
    this.offset += size;
    return decode(v);
  }

  readHash(): Hash {
    const digest = new Uint8Array(this.buff, this.offset, sha1Size);
    this.offset += sha1Size;
    return new Hash(digest);
  }
}

const initialBufferSize = 2048;

export class BinaryNomsWriter {
  buff: ArrayBuffer;
  dv: DataView;
  offset: number;
  length: number;

  constructor() {
    this.buff = new ArrayBuffer(initialBufferSize);
    this.dv = new DataView(this.buff, 0);
    this.offset = 0;
    this.length = this.buff.byteLength;
  }

  get data(): Uint8Array {
    // Callers now owns the copied data.
    return new Uint8Array(new Uint8Array(this.buff, 0, this.offset));
  }

  ensureCapacity(n: number): void {
    if (this.offset + n <= this.length) {
      return;
    }

    const oldData = new Uint8Array(this.buff);

    while (this.offset + n > this.length) {
      this.length *= 2;
    }
    this.buff = new ArrayBuffer(this.length);
    this.dv = new DataView(this.buff, 0);

    const a = new Uint8Array(this.buff);
    a.set(oldData);
  }

  writeBytes(v: Uint8Array): void {
    const size = v.byteLength;
    this.writeUint32(size);

    this.ensureCapacity(size);
    const a = new Uint8Array(this.buff, this.offset, size);
    a.set(v);
    this.offset += size;
  }

  writeUint8(v: number): void {
    this.ensureCapacity(1);
    this.dv.setUint8(this.offset, v);
    this.offset++;
  }

  writeUint32(v: number): void {
    this.ensureCapacity(4);
    this.dv.setUint32(this.offset, v, littleEndian);
    this.offset += 4;
  }

  writeUint64(v: number): void {
    invariant(v <= Number.MAX_SAFE_INTEGER);
    const v2 = (v / maxUInt32) | 0;
    const v1 = v % maxUInt32;
    this.writeUint32(v1);
    this.writeUint32(v2);
  }

  writeFloat64(v: number): void {
    this.ensureCapacity(8);
    this.dv.setFloat64(this.offset, v, littleEndian);
    this.offset += 8;
  }

  writeBool(v:boolean): void {
    this.writeUint8(v ? 1 : 0);
  }

  writeString(v: string): void {
    this.writeBytes(encode(v));
  }

  writeHash(h: Hash): void {
    this.ensureCapacity(sha1Size);
    const a = new Uint8Array(this.buff, this.offset, sha1Size);
    a.set(h.digest);
    this.offset += sha1Size;
  }
}
