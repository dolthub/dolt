/* @flow */

'use strict';

const Ref = require('./ref.js');

class Chunk {
  ref: Ref;
  data: string;

  constructor(data: string = '', ref: ?Ref) {
    this.data = data;
    this.ref = ref ? ref : Ref.fromData(data);
  }

  isEmpty(): boolean {
    return this.data.length === 0;
  }

  static emptyChunk: Chunk;
}

Chunk.emptyChunk = new Chunk();

module.exports = Chunk;
