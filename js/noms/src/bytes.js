// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import crypto from 'crypto';

// Note: Flow doesn't know that Buffer extends Uint8Array, thus all of the FlowIssues.

export function alloc(size: number): Uint8Array {
  // $FlowIssue
  return Buffer.alloc(size);
}

export function fromValues(values: number[]): Uint8Array {
  // $FlowIssue
  return Buffer.from(values);
}

export function fromString(s: string): Uint8Array {
  // $FlowIssue
  return Buffer.from(s);
}

export function toString(buff: Uint8Array): string {
  return buff.toString();
}

export function fromHexString(str: string): Uint8Array {
  // $FlowIssue
  return Buffer.from(str, 'hex');
}

export function toHexString(buff: Uint8Array): string {
  return buff.toString('hex');
}

export function grow(buff: Uint8Array, size: number): Uint8Array {
  const b = alloc(size);
  // $FlowIssue
  buff.copy(b);
  return b;
}

export function copy(source: Uint8Array, target: Uint8Array, targetStart: number = 0) {
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

/**
 * Slice returns a copy of parts of the bytes starting at `start` ending at `end` (exclusive).
 */
export function slice(buff: Uint8Array, start: number, end: number): Uint8Array {
  const v = alloc(end - start);
  // $FlowIssue
  buff.copy(v, 0, start, end);
  return v;
}

export function subarray(buff: Uint8Array, start: number, end: number): Uint8Array {
  // $FlowIssue
  return Buffer.from(buff.buffer, buff.byteOffset + start, end - start);
}

export function readUtf8(buff: Uint8Array, start: number, end: number): string {
  return buff.toString('utf8', start, end);
}

export function encodeUtf8(str: string, buff: Uint8Array, offset: number): number {
  // $FlowIssue
  return offset + buff.write(str, offset);
}

export function utf8ByteLength(str: string): number {
  return Buffer.byteLength(str);
}

export function compare(b1: Uint8Array, b2: Uint8Array): number {
  // $FlowIssue
  return b1.compare(b2);
}

/**
 * Returns the first 20 bytes of the sha512 of data.
 */
export function sha512(data: Uint8Array): Uint8Array {
  const hash = crypto.createHash('sha512');
  // $FlowIssue
  hash.update(data);
  // $FlowIssue
  return hash.digest().slice(0, 20);
}
