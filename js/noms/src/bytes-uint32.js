// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

export function writeUint32(buf: Uint8Array, value: number, offset: number): number {
  // Big Endian
  buf[offset] = value >>> 24;
  buf[offset + 1] = value >>> 16;
  buf[offset + 2] = value >>> 8;
  buf[offset + 3] = value;
  return offset + 4;
}

export function readUint32(buf: Uint8Array, offset: number): number {
  // Big Endian
  return buf[offset] * 0x1000000 +  // Don't shift here or we might get a negative number.
    (buf[offset + 1] << 16) +
    (buf[offset + 2] << 8) +
    buf[offset + 3];
}
