// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import crypto from 'crypto';

// TODO: Introduce `type Bytes = Buffer`.

export function alloc(size: number): Uint8Array {
  return Buffer.alloc(size);
}

export function fromValues(values: number[]): Uint8Array {
  return Buffer.from(values);
}

export function fromString(s: string): Uint8Array {
  return Buffer.from(s);
}

export function toString(buff: Uint8Array): string {
  return buff.toString();
}

export function fromHexString(str: string): Uint8Array {
  return Buffer.from(str, 'hex');
}

export function toHexString(buff: Uint8Array): string {
  return buff.toString('hex');
}

export function grow(buff: Uint8Array, size: number): Uint8Array {
  const b = alloc(size);
  // $FlowIssue: We are really using Buffer here.
  buff.copy(b);
  return b;
}

export function copy(source: Uint8Array, target: Uint8Array, targetStart: number = 0) {
  if (source instanceof Buffer) {
    // $FlowIssue: We are really using Buffer here.
    source.copy(target, targetStart);
    return;
  }

  for (let i = 0; i < source.length; i++) {
    target[targetStart++] = source[i];
  }
}

/**
 * Slice returns a copy of parts of the bytes starting at `start` ending at `end` (exclusive).
 */
export function slice(buff: Uint8Array, start: number, end: number): Uint8Array {
  const v = alloc(end - start);
  // $FlowIssue: We are really using Buffer here.
  buff.copy(v, 0, start, end);
  return v;
}

export function subarray(buff: Uint8Array, start: number, end: number): Uint8Array {
  return Buffer.from(buff.buffer, buff.byteOffset + start, end - start);
}

export function readUtf8(buff: Uint8Array, start: number, end: number): string {
  return buff.toString('utf8', start, end);
}

export function encodeUtf8(str: string, buff: Uint8Array, offset: number): number {
  // $FlowIssue: We are really using Buffer here.
  return offset + buff.write(str, offset);
}

export function utf8ByteLength(str: string): number {
  return Buffer.byteLength(str);
}

export function compare(b1: Uint8Array, b2: Uint8Array): number {
  // $FlowIssue: We are really using Buffer here.
  return b1.compare(b2);
}

/**
 * Returns the first 20 bytes of the sha512 of data.
 */
export function sha512(data: Uint8Array): Uint8Array {
  const hash = crypto.createHash('sha512');
  // $FlowIssue: We are really using Buffer here.
  hash.update(data);
  return hash.digest().slice(0, 20);
}
