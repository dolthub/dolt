/* @flow */

'use strict';

import Ref from './ref.js';

export default class Chunk {
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
