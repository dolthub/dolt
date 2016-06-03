// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// This is the Node.js version. The browser version is in ./browser/sha1.js.

import crypto from 'crypto';

export default function sha1(data: Uint8Array): Uint8Array {
  const hash = crypto.createHash('sha1');
  hash.update(data);
  return hash.digest();
}
