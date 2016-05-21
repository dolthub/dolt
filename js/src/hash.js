// @flow

import {hex} from './sha1.js';

export const sha1Size = 20;
const pattern = /^(sha1-[0-9a-f]{40})$/;

const sha1Prefix = 'sha1-';
const emtpyHashStr = sha1Prefix + '0'.repeat(40);

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

export default class Hash {
  _hashStr: string;

  constructor(hahsStr: string) {
    this._hashStr = hahsStr;
  }

  get hash(): Hash {
    return this;
  }

  get digest(): Uint8Array {
    return hexToUint8(this._hashStr.substring(5));
  }

  isEmpty(): boolean {
    return this._hashStr === emtpyHashStr;
  }

  equals(other: Hash): boolean {
    return this._hashStr === other._hashStr;
  }

  compare(other: Hash): number {
    return this._hashStr === other._hashStr ? 0 : this._hashStr < other._hashStr ? -1 : 1;
  }

  toString(): string {
    return this._hashStr;
  }

  static parse(s: string): Hash {
    const m = s.match(pattern);
    if (!m) {
      throw Error('Could not parse hash: ' + s);
    }

    return new Hash(m[1]);
  }

  static maybeParse(s: string): ?Hash {
    const m = s.match(pattern);
    return m ? new Hash(m[1]) : null;
  }

  static fromDigest(digest: Uint8Array = new Uint8Array(sha1Size)) {
    return new Hash(sha1Prefix + uint8ArrayToHex(digest));
  }

  static fromData(data: Uint8Array): Hash {
    return new Hash(sha1Prefix + hex(data));
  }
}

export const emptyHash = new Hash(emtpyHashStr);
