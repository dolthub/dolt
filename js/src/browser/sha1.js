// @flow

// This is the browser version. The Node.js version is in ../sha1.js.

import Rusha from 'rusha';

const r = new Rusha();

export function hex(data: Uint8Array): string {
  return r.digest(data);
}
