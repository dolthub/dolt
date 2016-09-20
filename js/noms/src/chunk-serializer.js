// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import Chunk from './chunk.js';
import Hash, {byteLength as hashByteLength} from './hash.js';
import {invariant} from './assert.js';
import * as Bytes from './bytes.js';
import {readUint32, writeUint32} from './bytes-uint32.js';

const headerSize = 4; // uint32
const chunkLengthSize = 4; // uint32
const chunkHeaderSize = hashByteLength + chunkLengthSize;

export type ChunkStream = (cb: (chunk: Chunk) => void) => Promise<void>

export function serialize(hints: Set<Hash>, stream: ChunkStream): Promise<Uint8Array> {
  const hintsLength = serializedHintsLength(hints);
  let buf = Bytes.alloc(Math.max(hintsLength * 2, 2048));
  let offset = 0;

  const ensureCapacity = (n: number) => {
    let length = buf.byteLength;
    if (offset + n <= length) {
      return;
    }

    while (offset + n > length) {
      length *= 2;
    }
    buf = Bytes.grow(buf, length);
  };

  offset = serializeHints(hints, buf);

  return stream(chunk => {
    const chunkLength = serializedChunkLength(chunk);
    ensureCapacity(chunkLength);
    offset = serializeChunk(chunk, buf, offset);
  }).then(() => Bytes.subarray(buf, 0, offset));
}

function serializeChunk(chunk: Chunk, buffer: Uint8Array, offset: number): number {
  invariant(buffer.byteLength - offset >= serializedChunkLength(chunk),
    'Invalid chunk buffer');

  Bytes.copy(chunk.hash.digest, buffer, offset);
  offset += hashByteLength;

  const chunkLength = chunk.data.length;
  offset = writeUint32(buffer, chunkLength, offset);

  Bytes.copy(chunk.data, buffer, offset);
  offset += chunkLength;
  return offset;
}

function serializeHints(hints: Set<Hash>, buff: Uint8Array): number {
  let offset = writeUint32(buff, hints.size, 0);

  hints.forEach(hash => {
    Bytes.copy(hash.digest, buff, offset);
    offset += hashByteLength;
  });

  return offset;
}

function serializedHintsLength(hints: Set<Hash>): number {
  return headerSize + hashByteLength * hints.size;
}

function serializedChunkLength(chunk: Chunk): number {
  return chunkHeaderSize + chunk.data.length;
}

export function deserialize(buff: Uint8Array): {hints: Array<Hash>, chunks: Array<Chunk>} {
  const {hints, offset} = deserializeHints(buff);
  return {hints: hints, chunks: deserializeChunks(buff, offset)};
}

function deserializeHints(buff: Uint8Array): {hints: Array<Hash>, offset: number} {
  const hints:Array<Hash> = [];

  let offset = 0;
  const numHints = readUint32(buff, offset);
  offset += headerSize;

  invariant(buff.byteLength - offset >= hashByteLength * numHints, 'Invalid hint buffer');
  for (let i = 0; i < numHints; i++) {
    const hash = new Hash(Bytes.slice(buff, offset, offset + hashByteLength)); // copy
    offset += hashByteLength;
    hints.push(hash);
  }

  return {hints: hints, offset: offset};
}

export function deserializeChunks(buff: Uint8Array, offset: number = 0):
    Array<Chunk> {
  const chunks:Array<Chunk> = [];

  const totalLength = buff.byteLength;
  for (; offset < totalLength;) {
    invariant(buff.byteLength - offset >= chunkHeaderSize, 'Invalid chunk buffer');

    // No need to copy the data out since we are not holding on to the hash object.
    const hash = new Hash(Bytes.subarray(buff, offset, offset + hashByteLength));
    offset += hashByteLength;

    const chunkLength = readUint32(buff, offset);
    offset += chunkLengthSize;

    invariant(offset + chunkLength <= totalLength, 'Invalid chunk buffer');
    const chunk = new Chunk(Bytes.slice(buff, offset, offset + chunkLength)); // copy

    invariant(chunk.hash.equals(hash), 'Serialized hash !== computed hash');

    offset += chunkLength;
    chunks.push(chunk);
  }

  return chunks;
}
