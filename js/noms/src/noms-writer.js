// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import type Hash from './hash.js';
import type {Type} from './type.js';

export interface NomsWriter {
  writeBytes(v: Uint8Array): void;
  writeUint8(v: number): void;
  writeUint32(v: number): void;
  writeUint64(v: number): void;
  writeNumber(v: number): void;
  writeBool(v: boolean): void;
  writeString(v: string): void;
  writeHash(h: Hash): void;
  appendType(t: Type<any>): void;
}
