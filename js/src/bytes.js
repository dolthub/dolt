// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import crypto from 'crypto';

// Note: Flow doesn't know that Buffer extends Uint8Array, thus all of the FlowIssues.
export default class Bytes {

  static alloc(size: number): Uint8Array {
    // $FlowIssue
    return Buffer.alloc(size);
  }

  static fromValues(values: number[]): Uint8Array {
    // $FlowIssue
    return Buffer.from(values);
  }

  static fromString(s: string): Uint8Array {
    // $FlowIssue
    return Buffer.from(s);
  }

  static toString(buff: Uint8Array): string {
    return buff.toString();
  }

  static fromHexString(str: string): Uint8Array {
    // $FlowIssue
    return Buffer.from(str, 'hex');
  }

  static toHexString(buff: Uint8Array): string {
    return buff.toString('hex');
  }

  static grow(buff: Uint8Array, size: number): Uint8Array {
    const b = Bytes.alloc(size);
    // $FlowIssue
    buff.copy(b);
    return b;
  }

  static copy(source: Uint8Array, target: Uint8Array, targetStart: number = 0) {
    // $FlowIssue
    if (source instanceof Buffer) {
      // $FlowIssue
      source.copy(target, targetStart);
      return;
    }

    for (let i = 0; i < source.length; i++) {
      target[targetStart++] = source[i];
    }
  }

  static slice(buff: Uint8Array, start: number, end: number): Uint8Array {
    const v = Bytes.alloc(end - start);
    // $FlowIssue
    buff.copy(v, 0, start, end);
    return v;
  }

  static subarray(buff: Uint8Array, start: number, end: number): Uint8Array {
    // $FlowIssue
    return Buffer.from(buff.buffer, buff.byteOffset + start, end - start);
  }

  static readUtf8(buff: Uint8Array, start: number, end: number): string {
    return buff.toString('utf8', start, end);
  }

  static encodeUtf8(str: string, buff: Uint8Array, dv: DataView, offset: number): number {
    const size = Buffer.byteLength(str);
    // $FlowIssue
    buff.writeUInt32LE(size, offset);
    offset += 4;

    // $FlowIssue
    buff.write(str, offset);
    offset += size;
    return offset;
  }

  static compare(b1: Uint8Array, b2: Uint8Array): number {
    // $FlowIssue
    return b1.compare(b2);
  }

  static sha1(data: Uint8Array): Uint8Array {
    const hash = crypto.createHash('sha1');
    hash.update(data);
    return hash.digest();
  }
}
