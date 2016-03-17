// @flow

import BuzHash from './buzhash.js';
import {invariant} from './assert.js';
import type {uint8} from './primitives.js';

type getDataFn<T> = (item: T) => (Uint8Array | uint8);

export default class BuzHashBoundaryChecker<T> {
  windowSize: number;
  valueSize: number;
  pattern: number;
  _hash: BuzHash;
  _getData: getDataFn<T>;

  constructor(windowSize: number, valueSize: number, pattern: number, getData: getDataFn) {
    this._hash = new BuzHash(windowSize * valueSize);
    this.windowSize = windowSize;
    this.valueSize = valueSize;
    this.pattern = pattern;
    this._getData = getData;
  }

  write(item: T): boolean {
    const data = this._getData(item);
    return typeof data === 'number' ? this._writeByte(data) : this._writeBytes(data);
  }

  _writeBytes(bytes: Uint8Array): boolean {
    invariant(this.valueSize === bytes.length);
    this._hash.write(bytes);
    return this._checkPattern();
  }

  _writeByte(b: uint8): boolean {
    this._hash.hashByte(b);
    return this._checkPattern();
  }

  _checkPattern(): boolean {
    const sum = this._hash.sum32;
    const result = (sum & this.pattern) | 0;
    return result === this.pattern;
  }
}
