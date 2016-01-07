// @flow

import Chunk from './chunk.js';
import Ref from './ref.js';
import {invariant} from './assert.js';

const sha1Size = 20;
const chunkLengthSize = 4; // uint32
const chunkHeaderSize = sha1Size + chunkLengthSize;

export function serialize(chunks: Array<Chunk>): ArrayBuffer {
  let totalSize = 0;
  for (let i = 0; i < chunks.length; i++) {
    totalSize += chunkHeaderSize + chunks[i].data.length;
  }

  const buffer = new ArrayBuffer(totalSize);
  let offset = 0;

  for (let i = 0; i < chunks.length; i++) {
    const chunk = chunks[i];
    const refArray = new Uint8Array(buffer, offset, sha1Size);
    refArray.set(chunk.ref.digest);
    offset += sha1Size;

    // Uint32Arrays cannot be created at non-4-byte offsets into a buffer, so read & write of
    // chunkLength must be done with tmp Uint8Array.
    const chunkLength = chunk.data.length;
    const sizeArray = new Uint32Array(1);
    sizeArray[0] = chunkLength;
    const sizeWriteArray = new Uint8Array(buffer, offset, chunkLengthSize);
    sizeWriteArray.set(new Uint8Array(sizeArray.buffer));
    offset += chunkLengthSize;

    const dataArray = new Uint8Array(buffer, offset, chunkLength);
    dataArray.set(chunk.data);
    offset += chunkLength;
  }

  return buffer;
}

export function deserialize(buffer: ArrayBuffer): Array<Chunk> {
  const chunks:Array<Chunk> = [];

  const totalLenth = buffer.byteLength;
  for (let offset = 0; offset < totalLenth;) {
    invariant(buffer.byteLength - offset >= chunkHeaderSize, 'Invalid chunk buffer');

    const refArray = new Uint8Array(buffer, offset, sha1Size);
    const ref = Ref.fromDigest(new Uint8Array(refArray));
    offset += sha1Size;

    const sizeReadArray = new Uint8Array(buffer, offset, chunkLengthSize);
    const sizeArray = new Uint32Array(new Uint8Array(sizeReadArray).buffer);
    const chunkLength = sizeArray[0];
    offset += chunkLengthSize;

    invariant(offset + chunkLength <= totalLenth, 'Invalid chunk buffer');

    const dataArray = new Uint8Array(buffer, offset, chunkLength);
    const chunk = new Chunk(new Uint8Array(dataArray)); // Makes a slice (copy) of the byte sequence
                                                        // from buffer.

    invariant(chunk.ref.equals(ref), 'Serialized ref !== computed ref');

    offset += chunkLength;
    chunks.push(chunk);
  }

  return chunks;
}
