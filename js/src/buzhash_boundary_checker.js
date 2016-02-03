// @flow

import BuzHash from './buzhash.js';
import {invariant} from './assert.js';

export type getBytesFn<T> = (item: T) => Uint8Array;

export default class BuzHashBoundaryChecker<T> {
  windowSize: number;
  valueSize: number;
  pattern: number;
  _hash: BuzHash;
  _getBytes: getBytesFn<T>;

  constructor(windowSize: number, valueSize: number, pattern: number, getBytes: getBytesFn) {
    this._hash = new BuzHash(windowSize * valueSize);
    this.windowSize = windowSize;
    this.valueSize = valueSize;
    this.pattern = pattern;
    this._getBytes = getBytes;
  }

  write(item: T): boolean {
    const bytes = this._getBytes(item);
    invariant(this.valueSize === bytes.length);
    this._hash.write(bytes);
    const sum = this._hash.sum32;
    const result = (sum & this.pattern) | 0;
    return result === this.pattern;
  }
}
