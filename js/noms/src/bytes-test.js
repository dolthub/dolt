// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {assert} from 'chai';
import {suite, test} from 'mocha';

import * as NodeBytes from './bytes.js';
import * as BrowserBytes from './browser/bytes.js';

function assertUint8Equal(ar1: Uint8Array, ar2: Uint8Array) {
  assert.equal(ar1.length, ar2.length);
  for (let i = 0; i < ar1.length; i++) {
    assert.equal(ar1[i], ar2[i]);
  }
}

function assertBytes(expect: number[], buff: Uint8Array) {
  const buffValues = [];
  for (let i = 0; i < buff.length; i++) {
    buffValues.push(buff[i]);
  }

  assert.deepEqual(expect, buffValues);
}

suite('Bytes', () => {
  test('alloc', () => {
    function test(size: number) {
      const n = NodeBytes.alloc(size);
      const b = BrowserBytes.alloc(size);
      assert.strictEqual(size, n.length);
      assert.strictEqual(size, b.length);
    }

    test(0);
    test(10);
    test(2048);
  });

  test('string', () => {
    function test(expect: number[], str: string) {
      assertBytes(expect, NodeBytes.fromString(str));
      assertBytes(expect, BrowserBytes.fromString(str));
      assert.strictEqual(str, NodeBytes.toString(NodeBytes.fromValues(expect)));
      assert.strictEqual(str, BrowserBytes.toString(BrowserBytes.fromValues(expect)));
    }

    test([], '');
    test([104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100], 'hello world');
    test([207, 128], '\u03c0');
  });

  test('hex', () => {
    function test(expect: number[], hexString: string) {
      assertBytes(expect, NodeBytes.fromHexString(hexString));
      assertBytes(expect, BrowserBytes.fromHexString(hexString));
      assert.strictEqual(hexString, NodeBytes.toHexString(NodeBytes.fromValues(expect)));
      assert.strictEqual(hexString, BrowserBytes.toHexString(BrowserBytes.fromValues(expect)));
    }

    test([], '');
    test([0x3e, 0x9a, 0x25, 0x9b], '3e9a259b');
    test([0x04, 0x03, 0x97, 0xca, 0x92, 0x1d, 0x02, 0x7d, 0xae, 0x6d,
          0xfc, 0x01, 0x02, 0xf2, 0x04, 0x46, 0x6f, 0xea, 0xd5, 0xc9],
          '040397ca921d027dae6dfc0102f204466fead5c9');
  });

  test('grow', () => {
    function test(bytes: any) {
      const buff = bytes.alloc(4);
      buff[0] = 1;
      buff[1] = 2;
      buff[2] = 3;
      const b2 = bytes.grow(buff, 8);
      assert.strictEqual(1, b2[0]);
      assert.strictEqual(2, b2[1]);
      assert.strictEqual(3, b2[2]);
      assert.strictEqual(8, b2.length);
    }

    test(NodeBytes);
    test(BrowserBytes);
  });

  test('copy', () => {
    function test(bytes: any) {
      const buff = bytes.alloc(3);
      buff[0] = 1;
      buff[1] = 2;
      buff[2] = 3;
      const b2 = bytes.alloc(6);
      b2[0] = 10;
      b2[1] = 11;
      b2[2] = 12;
      bytes.copy(buff, b2, 3);
      assert.strictEqual(10, b2[0]);
      assert.strictEqual(11, b2[1]);
      assert.strictEqual(12, b2[2]);
      assert.strictEqual(1, b2[3]);
      assert.strictEqual(2, b2[4]);
      assert.strictEqual(3, b2[5]);
    }

    test(NodeBytes);
    test(BrowserBytes);
  });

  test('slice', () => {
    function test(bytes: any) {
      const buff = bytes.alloc(4);
      buff[0] = 1;
      buff[1] = 2;
      buff[2] = 3;
      buff[3] = 4;

      const b2 = bytes.slice(buff, 1, 3);
      buff[2] = 4;

      assert.equal(b2.length, 2);
      assert.strictEqual(2, b2[0]);
      assert.strictEqual(3, b2[1]);
    }

    test(NodeBytes);
    test(BrowserBytes);
  });

  test('subarray', () => {
    function test(bytes: any) {
      const buff = bytes.alloc(3);
      buff[0] = 1;
      buff[1] = 2;
      buff[2] = 3;

      const b2 = bytes.subarray(buff, 1, 3);
      buff[2] = 4;

      assert.strictEqual(2, b2[0]);
      assert.strictEqual(4, b2[1]);
    }

    test(NodeBytes);
    test(BrowserBytes);
  });

  test('compare', () => {
    function test(bytes: any, expect: number, a1: number[], a2: number[]) {
      assert.strictEqual(expect, bytes.compare(bytes.fromValues(a1), bytes.fromValues(a2)));
    }

    test(NodeBytes, 0, [], []);
    test(BrowserBytes, 0, [], []);

    test(NodeBytes, 0, [1, 2, 3], [1, 2, 3]);
    test(BrowserBytes, 0, [1, 2, 3], [1, 2, 3]);

    test(NodeBytes, -1, [1, 2], [1, 2, 3]);
    test(BrowserBytes, -1, [1, 2], [1, 2, 3]);

    test(NodeBytes, 1, [1, 2, 3, 4], [1, 2, 3]);
    test(BrowserBytes, 1, [1, 2, 3, 4], [1, 2, 3]);

    test(NodeBytes, 1, [2, 2, 3], [1, 2, 3]);
    test(BrowserBytes, 1, [2, 2, 3], [1, 2, 3]);

    test(NodeBytes, -1, [0, 2, 3], [1, 2, 3]);
    test(BrowserBytes, -1, [0, 2, 3], [1, 2, 3]);
  });

  test('sha512', () => {
    function test(arr: number[]) {
      // Node uses a Buffer, browser uses a Uint8Array
      const n = NodeBytes.sha512(NodeBytes.fromValues(arr));
      const b = BrowserBytes.sha512(BrowserBytes.fromValues(arr));
      assertUint8Equal(n, b);
    }

    test([]);
    test([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    test([1, 2, 3, 4, 5]);
    test([2, 3, 4, 5, 10]);
  });

  test('encodeUtf8', () => {
    function t(str) {
      const nb = NodeBytes.alloc(100);
      const ndv = new DataView(nb.buffer, nb.byteOffset, nb.byteLength);
      const no = NodeBytes.encodeUtf8(str, nb, ndv, 0);

      const bb = BrowserBytes.alloc(100);
      const bdv = new DataView(bb.buffer, bb.byteOffset, bb.byteLength);
      const bo = BrowserBytes.encodeUtf8(str, bb, bdv, 0);

      assert.equal(no, bo);
      for (let i = 0; i < no; i++) {
        assert.equal(nb[i], bb[i]);
      }
    }
    t('');
    t('hello');
    t('💩');
  });
});
