// @flow

import {hex} from './sha1.js';

export const sha1Size = 20;
const pattern = /^(sha1-[0-9a-f]{40})$/;

const sha1Prefix = 'sha1-';
const emtpyRefStr = sha1Prefix + '0'.repeat(40);

function uint8ArrayToHex(a: Uint8Array): string {
  let hex = '';
  for (let i = 0; i < a.length; i++) {
    const v = a[i].toString(16);
    if (v.length === 1) {
      hex += '0' + v;
    } else {
      hex += v;
    }
  }

  return hex;
}

function hexToUint8(s: string): Uint8Array {
  const digest = new Uint8Array(sha1Size);
  for (let i = 0; i < sha1Size; i++) {
    const ch = s.substring(i * 2, i * 2 + 2);
    digest[i] = parseInt(ch, 16);
  }

  return digest;
}

export default class Ref {
  _refStr: string;

  constructor(refStr: string) {
    this._refStr = refStr;
  }

  get ref(): Ref {
    return this;
  }

  get digest(): Uint8Array {
    return hexToUint8(this._refStr.substring(5));
  }

  isEmpty(): boolean {
    return this._refStr === emtpyRefStr;
  }

  equals(other: Ref): boolean {
    return this._refStr === other._refStr;
  }

  compare(other: Ref): number {
    return this._refStr === other._refStr ? 0 : this._refStr < other._refStr ? -1 : 1;
  }

  toString(): string {
    return this._refStr;
  }

  static parse(s: string): Ref {
    const m = s.match(pattern);
    if (!m) {
      throw Error('Could not parse ref: ' + s);
    }

    return new Ref(m[1]);
  }

  static maybeParse(s: string): ?Ref {
    const m = s.match(pattern);
    return m ? new Ref(m[1]) : null;
  }

  static fromDigest(digest: Uint8Array = new Uint8Array(sha1Size)) {
    return new Ref(sha1Prefix + uint8ArrayToHex(digest));
  }

  static fromData(data: Uint8Array): Ref {
    return new Ref(sha1Prefix + hex(data));
  }
}

export const emptyRef = new Ref(emtpyRefStr);
