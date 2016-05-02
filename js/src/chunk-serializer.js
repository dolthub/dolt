// @flow

import Chunk from './chunk.js';
import Ref from './ref.js';
import {invariant} from './assert.js';

const headerSize = 4; // uint32
const littleEndian = true;
const sha1Size = 20;
const chunkLengthSize = 4; // uint32
const chunkHeaderSize = sha1Size + chunkLengthSize;

export function serialize(hints: Set<Ref>, chunks: Array<Chunk>): ArrayBuffer {
  const buffer = new ArrayBuffer(serializedHintLength(hints) + serializedChunkLength(chunks));

  let offset = serializeHints(hints, buffer);

  for (let i = 0; i < chunks.length; i++) {
    const chunk = chunks[i];
    invariant(buffer.byteLength - offset >= chunkHeaderSize + chunk.data.length,
      'Invalid chunk buffer');

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

function serializeHints(hints: Set<Ref>, buffer: ArrayBuffer): number {
  let offset = 0;
  const view = new DataView(buffer, offset, headerSize);
  view.setUint32(offset, hints.size | 0, littleEndian); // Coerce number to uint32
  offset += headerSize;

  hints.forEach(ref => {
    const refArray = new Uint8Array(buffer, offset, sha1Size);
    refArray.set(ref.digest);
    offset += sha1Size;
  });

  return offset;
}

function serializedHintLength(hints: Set<Ref>): number {
  return headerSize + sha1Size * hints.size;
}

function serializedChunkLength(chunks: Array<Chunk>): number {
  let totalSize = 0;
  for (let i = 0; i < chunks.length; i++) {
    totalSize += chunkHeaderSize + chunks[i].data.length;
  }
  return totalSize;
}

export function deserialize(buffer: ArrayBuffer): {hints: Array<Ref>, chunks: Array<Chunk>} {
  const {hints, offset} = deserializeHints(buffer);
  return {hints: hints, chunks: deserializeChunks(buffer, offset)};
}

function deserializeHints(buffer: ArrayBuffer): {hints: Array<Ref>, offset: number} {
  const hints:Array<Ref> = [];

  let offset = 0;
  const view = new DataView(buffer, 0, headerSize);
  const numHints = view.getUint32(0, littleEndian);
  offset += headerSize;

  const totalLength = headerSize + (numHints * sha1Size);
  for (; offset < totalLength;) {
    invariant(buffer.byteLength - offset >= sha1Size, 'Invalid hint buffer');

    const refArray = new Uint8Array(buffer, offset, sha1Size);
    const ref = Ref.fromDigest(new Uint8Array(refArray));
    offset += sha1Size;

    hints.push(ref);
  }

  return {hints: hints, offset: offset};
}

export function deserializeChunks(buffer: ArrayBuffer, offset: number = 0): Array<Chunk> {
  const chunks:Array<Chunk> = [];

  const totalLenth = buffer.byteLength;
  for (; offset < totalLenth;) {
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
