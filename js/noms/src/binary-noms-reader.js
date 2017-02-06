// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import * as Bytes from './bytes.js';
import Hash, {byteLength as hashByteLength} from './hash.js';
import type TypeCache from './type-cache.js';
import {BinaryReader} from './binary-rw.js';

export default class BinaryNomsReader extends BinaryReader {
  constructor(buff: Uint8Array) {
    super(buff);
  }

  readIdent(tc: TypeCache): number {
    const str = this.readString(); // TODO: Figure out how to do this without allocating.
    let id = tc.identTable.entries.get(str);
    if (id === undefined) {
      id = tc.identTable.getId(str);
    }

    return id;
  }

  readHash(): Hash {
    // Make a copy of the data.
    const digest = Bytes.slice(this.buff, this.offset, this.offset + hashByteLength);
    this.offset += hashByteLength;
    return new Hash(digest);
  }
}
