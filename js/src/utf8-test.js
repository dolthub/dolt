// @flow

import {suite, test} from 'mocha';
import {assert} from 'chai';

import {encode as encodeNative, decode as decodeNative} from './utf8.js';
import {encode as encodeBrowser, decode as decodeBrowser} from './browser/utf8.js';

suite('Utf8', () => {
  test('encode', () => {
    function assertSame(s: string) {
      assert.deepEqual(encodeNative(s), encodeBrowser(s));
    }

    assertSame('');
    assertSame('hello world');
    assertSame('\u03c0');
  });

  test('decode', () => {
    function assertSame(data: Uint8Array) {
      assert.strictEqual(decodeNative(data), decodeBrowser(data));
    }

    assertSame(new Uint8Array(0));
    assertSame(new Uint8Array([100, 101, 102]));
    assertSame(new Uint8Array([0x3c0]));
  });
});
