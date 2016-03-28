// @flow

// This is the Node.js version. The browser version is in ./browser/utf8.js.

// TODO: Use opaque type for the data. https://github.com/attic-labs/noms/issues/1082

export function encode(s: string): Uint8Array {
  const buf = new Buffer(s, 'utf8');
  const len = buf.length;
  const arr = new Uint8Array(len);
  for (let i = 0; i < len; i++) {
    arr[i] = buf[i];
  }
  return arr;
}

export function decode(data: Uint8Array): string {
  const len = data.length;
  const buf = new Buffer(len);
  for (let i = 0; i < len; i++) {
    buf[i] = data[i];
  }
  return buf.toString('utf8');
}
