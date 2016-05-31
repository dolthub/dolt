// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

// This is the Node.js version. The browser version is in ./browser/utf8.js.

// TODO: Use opaque type for the data. https://github.com/attic-labs/noms/issues/1082

export function encode(s: string): Uint8Array {
  const buf = new Buffer(s, 'utf8');
  // Note: it would be better not to copy the memory, and instead do:
  //
  // return new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength)
  //
  // but Node5 seems to get confused converting between Buffer and Uint8Array while trying to use
  // the same underlying buffer. Node6 works fine, but it's not the LTS release yet.
  return new Uint8Array(buf);
}

export function decode(data: Uint8Array): string {
  // Note: see comment above. For a similar reason, it would be better, but impossible, to do:
  //
  // return new Buffer(data.buffer, data.byteOffset, data.byteLength).toString('utf8');
  //
  // $FlowIssue: flow doesn't know this is a legit constructor.
  return new Buffer(data).toString('utf8');
}
