// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {SHA512} from 'asmcrypto.js-sha512';
import {byteLength, encode, decode} from './utf8.js';

export {byteLength as utf8ByteLength, encode as encodeUtf8, decode as toString} from './utf8.js';

export function alloc(size: number): Uint8Array {
  return new Uint8Array(size);
}

export function fromValues(values: number[]): Uint8Array {
  return new Uint8Array(values);
}

export function fromString(s: string): Uint8Array {
  const size = byteLength(s);
  const buf = alloc(size);
  encode(s, buf, 0);
  return buf;
}

export function fromHexString(s: string): Uint8Array {
  const length = s.length / 2;
  const buff = new Uint8Array(length);
  for (let i = 0; i < length; i++) {
    const hc = asciiToBinary(s.charCodeAt(2 * i));
    const lc = asciiToBinary(s.charCodeAt(2 * i + 1));
    buff[i] = hc << 4 | lc;
  }
  return buff;
}

export function toHexString(buff: Uint8Array): string {
  const hex = new Array(buff.byteLength * 2);
  for (let i = 0; i < buff.length; i++) {
    hex[i] = byteToAscii[buff[i]];
  }
  return hex.join('');
}

export function grow(buff: Uint8Array, size: number): Uint8Array {
  const b = new Uint8Array(size);
  b.set(buff);
  return b;
}

export function copy(source: Uint8Array, target: Uint8Array, targetStart: number = 0) {
  target.set(source, targetStart);
}

/**
 * Slice returns a copy of parts of the bytes starting at `start` ending at `end` (exclusive).
 */
export function slice(buff: Uint8Array, start: number, end: number): Uint8Array {
  // Safari does not have slice on typed arrays.
  return new Uint8Array(buff.buffer.slice(buff.byteOffset + start, buff.byteOffset + end));
}

export function subarray(buff: Uint8Array, start: number, end: number): Uint8Array {
  return buff.subarray(start, end);
}

export function readUtf8(buff: Uint8Array, start: number, end: number): string {
  return decode(buff.subarray(start, end));
}


export function compare(b1: Uint8Array, b2: Uint8Array): number {
  const b1Len = b1.byteLength;
  const b2Len = b2.byteLength;

  for (let i = 0; i < b1Len && i < b2Len; i++) {
    if (b1[i] < b2[i]) {
      return -1;
    }

    if (b1[i] > b2[i]) {
      return 1;
    }
  }

  if (b1Len < b2Len) {
    return -1;
  }
  if (b1Len > b2Len) {
    return 1;
  }

  return 0;
}

// This should be imported but this prevents the cyclic dependency.
const hashByteLength = 20;

export function sha512(data: Uint8Array): Uint8Array {
  const full: Uint8Array = SHA512.bytes(data);
  // Safari does not have slice on Uint8Array yet.
  return new Uint8Array(full.buffer, full.byteOffset, hashByteLength);
}

function asciiToBinary(cc: number): number {
  // This only accepts the char code for '0' - '9', 'a' - 'f'
  return cc - (cc <= 57 ? 48 : 87); // '9', '0', 'a' - 10
}

// Precompute '00' to 'ff'.
const byteToAscii = new Array(256);
for (let i = 0; i < 256; i++) {
  byteToAscii[i] = (i < 0x10 ? '0' : '') + i.toString(16);
}
