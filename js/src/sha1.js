// @flow

// This is the Node.js version. The browser version is in ./browser/sha1.js.

import crypto from 'crypto';

export function hex(data: Uint8Array | Buffer): string {
  let buf: Buffer;
  if (data instanceof Buffer) {
    buf = data;
  } else if (data instanceof Uint8Array) {
    // Note: Don't construct a Buffer from an empty ArrayBuffer with a byteOffset (even of 0) - see
    // https://github.com/nodejs/node/issues/4517. Node only fixed it for the fs.read case.
    const len = data.byteLength;
    // Note: Buffer accepts an ArrayBuffer in its constructor, see
    // https://nodejs.org/api/buffer.html#buffer_buffers_and_character_encodings and scroll down to
    // "Buffers and TypedArray".
    // $FlowIssue: the above.
    buf = len === 0 ? new Buffer(0) : new Buffer(data.buffer, data.byteOffset, len);
  } else {
    throw new Error(`Unsupported type ${data.constructor.name}`);
  }
  const hash = crypto.createHash('sha1');
  hash.update(buf);
  return hash.digest('hex');
}
