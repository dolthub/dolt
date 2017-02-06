// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import type Hash from './hash.js';
import type TypeCache from './type-cache.js';

export interface NomsReader {
  pos(): number;
  seek(pos: number): void;
  readBytes(): Uint8Array;
  readUint8(): number;
  readUint32(): number;
  readUint64(): number;
  readNumber(): number;
  readBool(): boolean;
  readString(): string;
  readIdent(tc: TypeCache): number;
  readHash(): Hash;
}
