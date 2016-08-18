// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {frexp, ldexp} from './frexp.js';

export class BinaryEncoderDecoder {

  // write n to buf, return number of bytes written
  encode(buf: Buffer, n: number): number {
    const [mantissa, exponent] = frexp(n);
    // console.log(`${n} = ${mantissa} * 2^${exponent}`);
    buf.writeDoubleBE(mantissa, 0);
    buf.writeInt32BE(exponent, 8);
    return 12;
  }

  // read from buf to return number
  decode(buf: Buffer): number {
    const mantissa = buf.readDoubleBE(0);
    const exponent = buf.readInt32BE(8);
    return ldexp(mantissa, exponent);
  }
}
