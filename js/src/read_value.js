// @flow

import Ref from './ref.js';
import Chunk from './chunk.js';
import type {ChunkStore} from './chunk_store.js';
import {notNull} from './assert.js';

type decodeFn = (chunk: Chunk, cs: ChunkStore) => Promise<any>
let decodeNomsValue: ?decodeFn = null;

export async function readValue(r: Ref, cs: ChunkStore): Promise<any> {
  let chunk = await cs.get(r);
  if (chunk.isEmpty()) {
    return null;
  }

  return notNull(decodeNomsValue)(chunk, cs);
}

export function setDecodeNomsValue(decode: decodeFn) {
  decodeNomsValue = decode;
}
