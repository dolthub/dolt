// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {frexp, ldexp} from './frexp.js';

export class BinaryIntEncoderDecoder {

  // write n to buf, return number of bytes written
  encode(buf: Buffer, n: number): number {
    if (Number.isInteger(n)) {
      buf.writeInt8(0, 0);
      buf.writeInt32BE(n, 1);
      return 5;
    } else {
      const [mantissa, exponent] = frexp(n);
      // console.log(`${n} = ${mantissa} * 2^${exponent}`);
      buf.writeInt8(1, 0);
      buf.writeDoubleBE(mantissa, 1);
      buf.writeInt32BE(exponent, 9);
      return 12;
    }
  }

  // read from buf to return number
  decode(buf: Buffer): number {
    const isInteger = buf.readInt8(0);
    if (isInteger === 0) {
      return buf.readInt32BE(1);
    }
    const mantissa = buf.readDoubleBE(0);
    const exponent = buf.readInt32BE(8);
    if (mantissa === 0) {
      return exponent;
    } else {
      return ldexp(mantissa, exponent);
    }
  }
}
