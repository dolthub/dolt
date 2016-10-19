// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

// This is based on https://github.com/chrisumbel/thirty-two which is

/*
Copyright (c) 2011, Chris Umbel

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

// The following changes have been done:
// 1. Change the alphabet to 0-9a-v
// 2. No padding. This is only meant to be used with Noms Hash.
// 3. Use Uin8Array to work in a browser
// 4. Flow/EsLint

import {alloc} from './bytes.js';

const charTable = '0123456789abcdefghijklmnopqrstuv';

export function encode(plain: Uint8Array): string {
  let i = 0;
  let shiftIndex = 0;
  let digit = 0;
  let encoded = '';
  const len = plain.length;

  // byte by byte isn't as pretty as quintet by quintet but tests a bit
  // faster. will have to revisit.
  while (i < len) {
    const current = plain[i];

    if (shiftIndex > 3) {
      digit = current & (0xff >> shiftIndex);
      shiftIndex = (shiftIndex + 5) % 8;
      digit = (digit << shiftIndex) | ((i + 1 < len) ? plain[i + 1] : 0) >> (8 - shiftIndex);
      i++;
    } else {
      digit = (current >> (8 - (shiftIndex + 5))) & 0x1f;
      shiftIndex = (shiftIndex + 5) % 8;
      if (shiftIndex === 0) {
        i++;
      }
    }

    encoded += charTable[digit];
  }

  // No padding!
  return encoded;
}

export function decode(encoded: string): Uint8Array {
  let shiftIndex = 0;
  let plainChar = 0;
  let plainPos = 0;
  const decoded = alloc(20);

  // byte by byte isn't as pretty as octet by octet but tests a bit faster. will have to revisit.
  for (let i = 0; i < encoded.length; i++) {
    const plainDigit = charCodeToNum(encoded.charCodeAt(i));

    if (shiftIndex <= 3) {
      shiftIndex = (shiftIndex + 5) % 8;

      if (shiftIndex === 0) {
        decoded[plainPos++] = plainChar | plainDigit;
        plainChar = 0;
      } else {
        plainChar |= 0xff & (plainDigit << (8 - shiftIndex));
      }
    } else {
      shiftIndex = (shiftIndex + 5) % 8;
      decoded[plainPos++] = plainChar | 0xff & (plainDigit >>> shiftIndex);
      plainChar = 0xff & (plainDigit << (8 - shiftIndex));
    }

  }
  return decoded;
}

function charCodeToNum(cc: number): number {
  // This only accepts the char code for '0' - '9', 'a' - 'v'
  return cc - (cc <= 57 ? 48 : 87); // '9', '0', 'a' - 10
}
