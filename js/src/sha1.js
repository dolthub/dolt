// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

// This is the Node.js version. The browser version is in ./browser/sha1.js.

import crypto from 'crypto';

export function hex(data: Uint8Array): string {
  const hash = crypto.createHash('sha1');
  hash.update(data);
  return hash.digest('hex');
}
