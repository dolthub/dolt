// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {
  encode as varintEncode,
  decode as varintDecode,
} from 'varint';

export class BinaryVarintEncoderDecoder {

  // write n to buf, return number of bytes written
  encode(buf: Buffer, n: number): number {
    varintEncode(n, buf, 0);
    return varintEncode.bytes;
  }

  // read from buf to return number
  decode(buf: Buffer): number {
    return varintDecode(buf, 0);
  }
}
