// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

// This is the browser version. The Node.js version is in ../utf8.js.

import {TextEncoder, TextDecoder} from './text-encoding.js';

const decoder = new TextDecoder();
const encoder = new TextEncoder();

export function encode(s: string): Uint8Array {
  return encoder.encode(s);
}

export function decode(data: Uint8Array): string {
  return decoder.decode(data);
}
