// @flow

import Ref from './ref.js';
import {emptyChunk, default as Chunk} from './chunk.js';

export default class MemoryStore {
  _data: { [key: string]: Chunk };
  _root: Ref;

  constructor() {
    this._data = Object.create(null);
    this._root = new Ref();
  }

  getRoot(): Promise<Ref> {
    return Promise.resolve(this._root);
  }

  updateRoot(current: Ref, last: Ref): Promise<boolean> {
    if (!this._root.equals(last)) {
      return Promise.resolve(false);
    }

    this._root = current;
    return Promise.resolve(true);
  }

  get(ref: Ref): Promise<Chunk> {
    let c = this._data[ref.toString()];
    if (!c) {
      c = emptyChunk;
    }

    return Promise.resolve(c);
  }

  has(ref: Ref): Promise<boolean> {
    return Promise.resolve(this._data[ref.toString()] !== undefined);
  }

  put(c: Chunk) {
    this._data[c.ref.toString()] = c;
  }

  get size(): number {
    return Object.keys(this._data).length;
  }

  close() {}
}
