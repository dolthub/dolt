// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import Hash, {sha1Size} from './hash.js';
import {decode} from './utf8.js';
import {invariant} from './assert.js';

const maxUInt32 = Math.pow(2, 32);
const littleEndian = true;

export default class BinaryNomsReader {
  buf: ArrayBuffer;
  dv: DataView;
  byteOffset: number;
  offset: number;
  length: number;

  constructor(data: Uint8Array) {
    this.buf = data.buffer;
    this.byteOffset = data.byteOffset;
    this.offset = 0;
    this.length = data.length;
    this.dv = new DataView(data.buffer, data.byteOffset, data.byteLength);
  }

  readBytes(): Uint8Array {
    const size = this.readUint32();
    return this._copyBytes(size);
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
    // No copy here.
    const v = new Uint8Array(this.buf, this.byteOffset + this.offset, size);
    this.offset += size;
    return decode(v);
  }

  readHash(): Hash {
    return new Hash(this._copyBytes(sha1Size));
  }

  _copyBytes(size: number): Uint8Array {
    const start = this.byteOffset + this.offset;
    const v = new Uint8Array(this.buf.slice(start, start + size));
    this.offset += size;
    return v;
  }
}
