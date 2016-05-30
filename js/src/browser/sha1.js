// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

// This is the browser version. The Node.js version is in ../sha1.js.

import Rusha from 'rusha';

const r = new Rusha();

export function hex(data: Uint8Array): string {
  return r.digest(data);
}
