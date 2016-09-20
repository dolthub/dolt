// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// This is based on ECMA 262 encodeURIComponent and the V8 implementation.

/**
 * This encodes `str` as UTF-8 and writes it into `buffer` starting at `index`. The returns the
 * index to write at, in other words it returns `index` plus number of writes written.
 */
export function encode(str: string, buffer: Uint8Array, index: number): number {
  const strLen = str.length;
  for (let k = 0; k < strLen; k++) {
    const cc1 = str.charCodeAt(k);
    if (cc1 >= 0xdc00 && cc1 <= 0xdfff) throw new Error('Invalid string');
    if (cc1 < 0xd800 || cc1 > 0xdbff) {
      index = encodeSingle(cc1, buffer, index);
    } else {
      k++;
      if (k === strLen) throw new Error('Invalid string');
      const cc2 = str.charCodeAt(k);
      if (cc2 < 0xdc00 || cc2 > 0xdfff) throw new Error('Invalid string');
      index = encodePair(cc1, cc2, buffer, index);
    }
  }
  return index;
}

function encodeSingle(cc: number, result: Uint8Array, index: number): number {
  const x = (cc >> 12) & 0xf;
  const y = (cc >> 6) & 63;
  const z = cc & 63;
  if (cc <= 0x007f) {
    result[index++] = cc;
  } else if (cc <= 0x07ff) {
    result[index++] = y + 192;
    result[index++] = z + 128;
  } else {
    result[index++] = x + 224;
    result[index++] = y + 128;
    result[index++] = z + 128;
  }
  return index;
}

function encodePair(cc1: number, cc2: number, result: Uint8Array, index: number): number {
  const u = ((cc1 >> 6) & 0xf) + 1;
  const w = (cc1 >> 2) & 0xf;
  const x = cc1 & 3;
  const y = (cc2 >> 6) & 0xf;
  const z = cc2 & 63;
  result[index++] = (u >> 2) + 240;
  result[index++] = (((u & 3) << 4) | w) + 128;
  result[index++] = ((x << 4) | y) + 128;
  result[index++] = z + 128;
  return index;
}

function singleLength(cc: number, index: number): number {
  if (cc <= 0x007f) {
    index++;
  } else if (cc <= 0x07ff) {
    index += 2;
  } else {
    index += 3;
  }
  return index;
}

/**
 * The number of bytes needed if `str` is encoded using UTF-8.
 */
export function byteLength(str: string): number {
  const strLen = str.length;
  let length = 0;
  for (let k = 0; k < strLen; k++) {
    const cc1 = str.charCodeAt(k);
    if (cc1 >= 0xdc00 && cc1 <= 0xdfff) throw new Error('Invalid string');
    if (cc1 < 0xd800 || cc1 > 0xdbff) {
      length = singleLength(cc1, length);
    } else {
      k++;
      if (k === strLen) throw new Error('Invalid string');
      const cc2 = str.charCodeAt(k);
      if (cc2 < 0xdc00 || cc2 > 0xdfff) throw new Error('Invalid string');
      length += 4;
    }
  }
  return length;
}

/**
 * Decodes UTF-8 encoded `data` to a string.
 */
export function decode(data: Uint8Array): string {
  const dataLength = data.length;
  let k = 0;
  const charCodes = [];
  let index = 0;
  let o0 = 0;
  let o1 = 0;
  let o2 = 0;
  let o3 = 0;
  for (; k < dataLength; k++) {
    const cc = data[k];
    if (cc >> 7) {
      let n = 0;
      while (((cc << ++n) & 0x80) !== 0);
      if (n === 1 || n > 4) throw new Error('Invalid string');
      o0 = cc;
      if (k + 1 * (n - 1) >= dataLength) throw new Error('Invalid string');
      if (n > 1) o1 = data[++k];
      if (n > 2) o2 = data[++k];
      if (n > 3) o3 = data[++k];
      index = decodeOctets(o0, o1, o2, o3, charCodes, index);
    } else {
      charCodes[index++] = cc;
    }
  }
  return String.fromCharCode(...charCodes);
}

function decodeOctets(o0: number, o1: number, o2: number, o3: number,
                      result: number[], index: number): number {
  let value;
  if (o0 < 0x80) {
    value = o0;
  } else if (o0 < 0xc2) {
    throw new Error('Invalid string');
  } else {
    if (o0 < 0xe0) {
      const a = o0 & 0x1f;
      if ((o1 < 0x80) || (o1 > 0xbf)) {
        throw new Error('Invalid string');
      }
      const b = o1 & 0x3f;
      value = (a << 6) + b;
      if (value < 0x80 || value > 0x7ff) {
        throw new Error('Invalid string');
      }
    } else {
      if (o0 < 0xf0) {
        const a = o0 & 0x0f;
        if ((o1 < 0x80) || (o1 > 0xbf)) {
          throw new Error('Invalid string');
        }
        const b = o1 & 0x3f;
        if ((o2 < 0x80) || (o2 > 0xbf)) {
          throw new Error('Invalid string');
        }
        const c = o2 & 0x3f;
        value = (a << 12) + (b << 6) + c;
        if ((value < 0x800) || (value > 0xffff)) {
          throw new Error('Invalid string');
        }
      } else {
        if (o0 < 0xf8) {
          const a = o0 & 0x07;
          if ((o1 < 0x80) || (o1 > 0xbf)) {
            throw new Error('Invalid string');
          }
          const b = o1 & 0x3f;
          if ((o2 < 0x80) || (o2 > 0xbf)) {
            throw new Error('Invalid string');
          }
          const c = o2 & 0x3f;
          if ((o3 < 0x80) || (o3 > 0xbf)) {
            throw new Error('Invalid string');
          }
          const d = o3 & 0x3f;
          value = (a << 18) + (b << 12) + (c << 6) + d;
          if ((value < 0x10000) || (value > 0x10ffff)) {
            throw new Error('Invalid string');
          }
        } else {
          throw new Error('Invalid string');
        }
      }
    }
  }
  if (0xd800 <= value && value <= 0xdfff) {
    throw new Error('Invalid string');
  }
  if (value < 0x10000) {
    result[index++] = value;
  } else {
    result[index++] = (value >> 10) + 0xd7c0;
    result[index++] = (value & 0x3ff) + 0xdc00;
  }
  return index;
}
