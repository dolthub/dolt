// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

export class StringEncoderDecoder {

  // write n to buf, return number of bytes written
  encode(buf: Buffer, n: number): number {
    if (n < 1e20) {
      // $FlowIssue: Buffer.prototype.write returns a number
      return buf.write(n.toString(10));
    }
    // $FlowIssue: Buffer.prototype.write returns a number
    return buf.write(n.toExponential());
  }

  // read from buf to return number
  decode(buf: Buffer): number {
    const s = buf.toString();
    return parseFloat(s);
  }
}
