// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import Hash, {sha1Size} from './hash.js';
import {invariant} from './assert.js';

const maxUInt32 = Math.pow(2, 32);
const initialBufferSize = 2 * 1024;

export default class BinaryNomsWriter {
  buf: Buffer;
  offset: number;
  length: number;

  constructor() {
    // $FlowIssue: Flow does not know about allocUnsafe
    this.buf = Buffer.allocUnsafe(initialBufferSize);
    this.offset = 0;
    this.length = initialBufferSize;
  }

  /**
   * Returns a view of the underlying buffer data. The underlying ArrayBuffer might be larger than
   * the returned Uint8Array.
   */
  get data(): Uint8Array {
    // $FlowIssue: Flow does not know Buffer extends Uint8Array.
    return this.buf.slice(0, this.offset);
  }

  ensureCapacity(n: number): void {
    if (this.offset + n <= this.length) {
      return;
    }

    const oldBuf = this.buf;

    while (this.offset + n > this.length) {
      this.length *= 2;
    }
    // $FlowIssue: Flow does not know about allocUnsafe
    this.buf = Buffer.allocUnsafe(this.length);
    oldBuf.copy(this.buf);
  }

  writeBytes(v: Uint8Array): void {
    const size = v.byteLength;
    this.writeUint32(size);
    this.writeBytesWithoutPrefix(v, size);
  }

  writeUint8(v: number): void {
    this.ensureCapacity(1);
    this.offset = this.buf.writeUInt8(v, this.offset);
  }

  writeUint32(v: number): void {
    this.ensureCapacity(4);
    this.offset = this.buf.writeUInt32LE(v, this.offset);
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
    this.offset = this.buf.writeDoubleLE(v, this.offset);
  }

  writeBool(v:boolean): void {
    this.writeUint8(v ? 1 : 0);
  }

  writeString(v: string): void {
    const byteLength = Buffer.byteLength(v);
    this.writeUint32(byteLength);

    this.ensureCapacity(byteLength);

    // Unlike other write methods write returns the number of bytes written and does not include
    // the offset
    this.buf.write(v, this.offset, byteLength);
    this.offset += byteLength;
  }

  writeHash(h: Hash): void {
    this.writeBytesWithoutPrefix(h.digest, sha1Size);
  }

  writeBytesWithoutPrefix(v: Uint8Array, size: number) {
    this.ensureCapacity(size);
    copy(this.buf, v, this.offset);
    this.offset += size;
  }
}

function copy(dst: Buffer, src: Buffer | Uint8Array, offset: number) {
  if (src instanceof Buffer) {
    src.copy(dst, offset);
  } else {
    // $FlowIssue: Flow does not know Buffer extends Uint8Array.
    dst.set(src, offset);
  }
}
