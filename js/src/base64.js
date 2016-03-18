// @flow

// Based on https://github.com/niklasvh/base64-arraybuffer
//
// base64-arraybuffer
// https://github.com/niklasvh/base64-arraybuffer
//
// Copyright (c) 2012 Niklas von Hertzen
// Licensed under the MIT license.

const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';

// Build charCode -> index
const lookup: Uint8Array = new Uint8Array(256);
for (let i = 0 ; i < chars.length; i++) {
  lookup[chars.charCodeAt(i)] = i;
}

export function encode(bytes: Uint8Array): string {
  const len = bytes.length;
  let base64 = '';

  for (let i = 0; i < len; i += 3) {
    base64 += chars[bytes[i] >> 2];
    base64 += chars[((bytes[i] & 3) << 4) | (bytes[i + 1] >> 4)];
    base64 += chars[((bytes[i + 1] & 15) << 2) | (bytes[i + 2] >> 6)];
    base64 += chars[bytes[i + 2] & 63];
  }

  if (len % 3 === 2) {
    base64 = base64.substring(0, base64.length - 1) + '=';
  } else if (len % 3 === 1) {
    base64 = base64.substring(0, base64.length - 2) + '==';
  }

  return base64;
}

export function decode(s: string): Uint8Array {
  let bufferLength = s.length * 0.75;
  const len = s.length;

  if (s[len - 1] === '=') {
    bufferLength--;
    if (s[len - 2] === '=') {
      bufferLength--;
    }
  }

  const bytes = new Uint8Array(bufferLength);
  let p = 0;

  for (let i = 0; i < len; i += 4) {
    const encoded1 = lookup[s.charCodeAt(i)];
    const encoded2 = lookup[s.charCodeAt(i + 1)];
    const encoded3 = lookup[s.charCodeAt(i + 2)];
    const encoded4 = lookup[s.charCodeAt(i + 3)];

    bytes[p++] = (encoded1 << 2) | (encoded2 >> 4);
    bytes[p++] = ((encoded2 & 15) << 4) | (encoded3 >> 2);
    bytes[p++] = ((encoded3 & 3) << 6) | (encoded4 & 63);
  }

  return bytes;
}
