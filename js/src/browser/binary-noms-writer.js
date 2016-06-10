// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// This is the browser version. The Node.js version is in ../binary-noms-writer.js.

import Hash, {sha1Size} from '../hash.js';
import {encode} from '../utf8.js';
import {invariant} from '../assert.js';

const maxUInt32 = Math.pow(2, 32);
const littleEndian = true;
const initialBufferSize = 2 * 1024;

export default class BinaryNomsWriter {
  buf: ArrayBuffer;
  dv: DataView;
  offset: number;
  length: number;

  constructor() {
    this.buf = new ArrayBuffer(initialBufferSize);
    this.dv = new DataView(this.buf, 0);
    this.offset = 0;
    this.length = initialBufferSize;
  }

  /**
   * Returns a view of the underlying buffer data. The underlying ArrayBuffer might be larger than
   * the returned Uint8Array.
   */
  get data(): Uint8Array {
    // Callers now owns the copied data.
    return new Uint8Array(this.buf.slice(0, this.offset));
  }

  ensureCapacity(n: number): void {
    if (this.offset + n <= this.length) {
      return;
    }

    const oldData = new Uint8Array(this.buf);

    while (this.offset + n > this.length) {
      this.length *= 2;
    }
    this.buf = new ArrayBuffer(this.length);
    this.dv = new DataView(this.buf, 0);

    const a = new Uint8Array(this.buf);
    a.set(oldData);
  }

  writeBytes(v: Uint8Array): void {
    const size = v.byteLength;
    this.writeUint32(size);
    this.writeBytesWithoutPrefix(v, size);
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
    this.writeBytesWithoutPrefix(h.digest, sha1Size);
  }

  writeBytesWithoutPrefix(v: Uint8Array, size: number) {
    this.ensureCapacity(size);
    const a = new Uint8Array(this.buf, this.offset, size);
    a.set(v);
    this.offset += size;
  }
}
