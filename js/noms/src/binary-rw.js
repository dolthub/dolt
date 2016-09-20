// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import * as Bytes from './bytes.js';
import Hash, {byteLength as hashByteLength} from './hash.js';
import {floatToIntExp, intExpToFloat} from './number-util.js';
import {invariant} from './assert.js';
import {encode as encodeVarint, decode as decodeVarint, maxVarintLength} from './signed-varint.js';
import {readUint32, writeUint32} from './bytes-uint32.js';

export const maxUint32 = Math.pow(2, 32);

export class BinaryReader {
  buff: Uint8Array;
  offset: number;

  constructor(buff: Uint8Array) {
    this.buff = buff;
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
    const v = this.buff[this.offset];
    this.offset++;
    return v;
  }

  readUint32(): number {
    const v = readUint32(this.buff, this.offset);
    this.offset += 4;
    return v;
  }

  readUint64(): number {
    // Big endian
    const msi = this.readUint32();
    const lsi = this.readUint32();
    const v = msi * maxUint32 + lsi;
    invariant(v <= Number.MAX_SAFE_INTEGER);
    return v;
  }

  readNumber(): number {
    const intRes = decodeVarint(this.buff, this.offset);
    this.offset += intRes[1];
    const expRes = decodeVarint(this.buff, this.offset);
    this.offset += expRes[1];
    return intExpToFloat(intRes[0], expRes[0]);
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

  readHash(): Hash {
    // Make a copy of the data.
    const digest = Bytes.slice(this.buff, this.offset, this.offset + hashByteLength);
    this.offset += hashByteLength;
    return new Hash(digest);
  }
}

const initialBufferSize = 16;

export class BinaryWriter {
  buff: Uint8Array;
  offset: number;

  constructor() {
    this.buff = Bytes.alloc(initialBufferSize);
    this.offset = 0;
  }

  get data(): Uint8Array {
    // Callers now owns the copied data.
    return Bytes.slice(this.buff, 0, this.offset);
  }

  reset(): void {
    this.offset = 0;
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
    this.buff[this.offset] = v;
    this.offset++;
  }

  writeUint32(v: number): void {
    this.ensureCapacity(4);
    writeUint32(this.buff, v, this.offset);
    this.offset += 4;
  }

  writeUint64(v: number): void {
    invariant(v <= Number.MAX_SAFE_INTEGER);
    const msi = (v / maxUint32) | 0;
    const lsi = v % maxUint32;
    // Big endian
    this.writeUint32(msi);
    this.writeUint32(lsi);
  }

  writeNumber(v: number): void {
    const [intVal, expVal] = floatToIntExp(v);
    this.ensureCapacity(2 * maxVarintLength);
    this.offset += encodeVarint(intVal, this.buff, this.offset);
    this.offset += encodeVarint(expVal, this.buff, this.offset);
  }

  writeBool(v:boolean): void {
    this.writeUint8(v ? 1 : 0);
  }

  writeString(v: string): void {
    const len = Bytes.utf8ByteLength(v);
    this.writeUint32(len);
    this.ensureCapacity(len);
    this.offset = Bytes.encodeUtf8(v, this.buff, this.offset);
  }

  writeHash(h: Hash): void {
    this.ensureCapacity(hashByteLength);
    Bytes.copy(h.digest, this.buff, this.offset);
    this.offset += hashByteLength;
  }
}
