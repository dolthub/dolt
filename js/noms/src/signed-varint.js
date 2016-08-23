// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

export const maxVarintLength = 10;

const mathPowTwoThirtyTwo = Math.pow(2, 32);

function toUint32(n: number): number {
  return n >>> 0;
}

/**
 * Encodes `val` as signed varint and writes that into `buf` at `offset`. This returns the number
 * of bytes written.
 */
export function encode(val: number, buf: Uint8Array, offset: number): number {
  const val2 = val >= 0 ? val : -val;
  let hi = toUint32(val2 / mathPowTwoThirtyTwo);
  let lo = toUint32(val2);
  // Shift left 1
  // Get the highest n bits of lo
  const carry = lo >>> (32 - 1);
  lo = toUint32(lo << 1);
  hi = (hi << 1) | carry;  // no way that this can turn negative.
  if (val < 0) {
    if (lo !== 0) {
      lo--;
    } else {
      hi--;
      lo = 0xffffffff;
    }
  }

  const sigbits = hi !== 0 ? 64 - Math.clz32(hi) : lo === 0 ? 1 : 32 - Math.clz32(lo);
  const byteLength = Math.ceil(sigbits / 7);
  let j = offset;
  for (let i = 0; i < sigbits; i += 7) {
    if (i !== 0) {
      buf[j - 1] |= 0x80;
    }
    if (i < 28) {
      buf[j++] = (lo & (0x7f << i)) >>> i;
    } else if (i < 35) {
      buf[j++] = lo >>> 32 - 4 | (hi & 0b111) << 4;
    } else {
      buf[j++] = (hi & (0x7f << i)) >>> i;
    }
  }
  return byteLength;
}

/**
 * Decodes a signed varint from `buf`, starting at `offset`. This returns an array of the number and
 * the number of bytes consumed.
 */
export function decode(buf: Uint8Array, offset: number): [number, number] {
  let hi = 0, lo = 0, shift = 0, count;
  for (let i = offset; i < buf.length; i++) {
    const b = buf[i];
    if (shift < 28) {
      // lo
      lo |= (b & 0x7f) << shift;
    } else if (shift < 35) {
      // overlap
      lo |= (b & 0x7f) << 28;
      hi |= (b & 0x7f) >>> 4;
    } else {
      // hi
      hi |= (b & 0x7f) << shift - 32;
    }

    if (b < 0x80) {  // last one
      count = i - offset + 1;
      break;
    }

    shift += 7;
  }

  if (count === undefined) {
    throw new Error('Invalid number encoding');
  }

  let sign = 2;
  // lo can become negative due to `|`
  lo = toUint32(lo);
  if (lo & 1) {
    sign = -2;
    lo++;
  }

  return [(mathPowTwoThirtyTwo * hi + lo) / sign, count];
}

/**
 * The number of bytes needed to encode `n` as a signed varint.
 */
export function encodingLength(n: number): number {
  // Signed varints are encoded using zigzag encoding, where the number is multiplied by 2 and if
  // negative then the sign is removed and we subtract 1 (v >= 0 ? 2 * v : -2 * v - 1)

  if (n === 0) {
    return 1;
  }

  const neg = n < 0;
  if (neg) {
    n = -n;
  }

  const log2 = Math.log2(n);
  const log2Truncated = log2 | 0;
  let bits = log2Truncated + 2;  // log2(0b100) == 2, uses 3 bits, log2(0b101) == 2.32, uses 3 bits,
                                 // Multiply by 2 adds another bit.

  // If negative we need to subtract 1, if the number was an exact power of 2 then the significant
  // bits gets reduced, for example 0b1000 - 1 = 0b111
  if (neg && log2 === log2Truncated) {
    bits--;
  }

  // Now we have the number of bits needed to represent the number. The encoding we use uses 7 bits
  // per byte and sets the high bit if there are more numbers.
  return Math.ceil(bits / 7);
}
