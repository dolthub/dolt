/* @flow */

'use strict';

import Chunk from './chunk.js';
import Ref from './ref.js';

const sha1Size = 20;
const chunkLengthSize = 4; // uint32
const chunkHeaderSize = sha1Size + chunkLengthSize;

export function serialize(chunks: Array<Chunk>): ArrayBuffer {
  let totalSize = 0;
  for (let i = 0; i < chunks.length; i++) {
    totalSize += chunkHeaderSize + chunks[i].data.length;
  }

  let buffer = new ArrayBuffer(totalSize);
  let offset = 0;

  for (let i = 0; i < chunks.length; i++) {
    let chunk = chunks[i];
    let refArray = new Uint8Array(buffer, offset, sha1Size);
    refArray.set(chunk.ref.digest);
    offset += sha1Size;

    // Uint32Arrays cannot be created at non-4-byte offsets into a buffer, so read & write of chunkLength must be done with tmp Uint8Array.
    let chunkLength = chunk.data.length;
    let sizeArray = new Uint32Array(1);
    sizeArray[0] = chunkLength;
    let sizeWriteArray = new Uint8Array(buffer, offset, chunkLengthSize);
    sizeWriteArray.set(new Uint8Array(sizeArray.buffer));
    offset += chunkLengthSize;

    let dataArray = new Uint8Array(buffer, offset, chunkLength);
    dataArray.set(chunk.data);
    offset += chunkLength;
  }

  return buffer;
}

export function deserialize(buffer: ArrayBuffer): Array<Chunk> {
  let chunks:Array<Chunk> = [];

  let totalLenth = buffer.byteLength;
  for (let offset = 0; offset < totalLenth;) {
    if (buffer.byteLength - offset < chunkHeaderSize) {
      throw new Error('Invalid chunk buffer');
    }

    let refArray = new Uint8Array(buffer, offset, sha1Size);
    let ref = new Ref(new Uint8Array(refArray));
    offset += sha1Size;

    let sizeReadArray = new Uint8Array(buffer, offset, chunkLengthSize);
    let sizeArray = new Uint32Array(new Uint8Array(sizeReadArray).buffer);
    let chunkLength = sizeArray[0];
    offset += chunkLengthSize;

    if (offset + chunkLength > totalLenth) {
      throw new Error('Invalid chunk buffer');
    }

    let dataArray = new Uint8Array(buffer, offset, chunkLength);
    let chunk = new Chunk(new Uint8Array(dataArray)); // Makes a slice (copy) of the byte sequence from buffer.
    if (!chunk.ref.equals(ref)) {
      throw new Error('Serialized ref !== computed ref');
    }

    offset += chunkLength;
    chunks.push(chunk);
  }

  return chunks;
}

