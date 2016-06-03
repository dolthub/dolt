// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {assert} from 'chai';
import {suite, test} from 'mocha';

import sha1Node from './sha1.js';
import sha1Browser from './browser/sha1.js';

suite('Sha1', () => {
  test('hex', () => {
    function assertSame(arr: Uint8Array) {
      // Node uses a Buffer, browser uses a Uint8Array
      const n = sha1Node(arr);
      const b = sha1Browser(arr);
      assert.equal(n.byteLength, b.byteLength);
      const n2 = new Uint8Array(n.buffer, n.byteOffset, n.byteLength);
      const b2 = new Uint8Array(b.buffer, b.byteOffset, b.byteLength);
      for (let i = 0; i < n2.length; i++) {
        assert.equal(n2[i], b2[i]);
      }
    }

    assertSame(new Uint8Array(0));
    assertSame(new Uint8Array(42));

    const arr = new Uint8Array([1, 2, 3, 4, 5]);
    assertSame(arr);
    assertSame(new Uint8Array(arr));
    assertSame(new Uint8Array(arr.buffer));
    assertSame(new Uint8Array(arr.buffer, 1, 2));
  });
});
