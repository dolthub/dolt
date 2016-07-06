// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

interface EncoderDecoder {
  encode(buf: Buffer, n: number): number; // write n to buf, return number of bytes written
  decode(buf: Buffer): number; // read from buf to return number
}
