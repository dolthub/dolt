/* @flow */

'use strict';

const Ref = require('./ref.js');
const Chunk = require('./chunk.js');

class MemoryStore {
  _data: { [key: string]: Chunk };
  _root: Ref;

  constructor() {
    this._data = Object.create(null);
    this._root = new Ref();
  }

  get root(): Ref {
    return this._root;
  }

  updateRoot(current: Ref, last: Ref): boolean {
    if (!this._root.equals(last)) {
      return false
    }

    this._root = current;
    return true;
  }

  get(ref: Ref): Chunk {
    var c = this._data[ref.toString()];
    if (c == null) {
      c = Chunk.emptyChunk;
    }
    return c;
  }

  has(ref: Ref): boolean {
    return this._data[ref.toString()] == null;
  }

  put(c: Chunk) {
    this._data[c.ref.toString()] = c;
  }

  get size(): number {
    return Object.keys(this._data).length;
  }

  close() {}
}

module.exports = MemoryStore;
