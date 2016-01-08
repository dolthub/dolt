// @flow

import Rusha from 'rusha';

import type {Value} from './value.js';
import {invariant} from './assert.js';

const r = new Rusha();
const sha1Size = 20;
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

  constructor(refStr: string = emtpyRefStr) {
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

  equals(other: Value): boolean {
    invariant(other instanceof Ref);
    return this._refStr === other._refStr;
  }

  get chunks(): Array<Ref> {
    return [this];
  }

  less(other: Value): boolean {
    invariant(other instanceof Ref);
    return this._refStr < other._refStr;
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

  static fromDigest(digest: Uint8Array = new Uint8Array(sha1Size)) {
    return new Ref(sha1Prefix + uint8ArrayToHex(digest));
  }

  static fromData(data: Uint8Array): Ref {
    return new Ref(sha1Prefix + r.digest(data));
  }
}
