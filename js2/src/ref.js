/* @flow */

'use strict';

const Rusha = require('rusha');

const r = new Rusha();
const sha1Size = 20;
const pattern = /^sha1-([0-9a-f]{40})$/;

function uint8ArrayToHex(a: Uint8Array): string {
  let hex = '';
  for (let i = 0; i < a.length; i++) {
    let v = a[i].toString(16);
    if (v.length === 1) {
      hex += '0' + v;
    } else {
      hex += v;
    }
  }

  return hex;
}

function hexToUint8(s: string): Uint8Array {
  let digest = new Uint8Array(sha1Size);
  for (let i = 0; i < sha1Size; i++) {
    let ch = s.substring(i*2, i*2 + 2);
    digest[i] = parseInt(ch, 16);
  }

  return digest;
}

class Ref {
  digest: Uint8Array;

  constructor(digest: Uint8Array = new Uint8Array(sha1Size)) {
    this.digest = digest;
  }

  isEmpty(): boolean {
    for (let i = 0; i < sha1Size; i++) {
      if (this.digest[i] !== 0) {
        return false;
      }
    }

    return true;
  }

  equals(other: Ref): boolean {
    for (let i = 0; i < sha1Size; i++) {
      if (this.digest[i] !== other.digest[i]) {
        return false;
      }
    }

    return true;
  }

  toString(): string {
    return 'sha1-' + uint8ArrayToHex(this.digest);
  }

  static parse(s: string): Ref {
    let m = s.match(pattern);
    if (!m) {
      throw Error('Could not parse ref: ' + s);
    }

    return new Ref(hexToUint8(m[1]));
  }

  static fromData(data: string): Ref {
    let digest = r.rawDigest(data);
    return new Ref(new Uint8Array(digest.buffer));
  }
}

module.exports = Ref;
