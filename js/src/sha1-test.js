// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';

import {hex as hexNode} from './sha1.js';
import {hex as hexBrowser} from './browser/sha1.js';

suite('Sha1', () => {
  test('hex', () => {
    function assertSame(arr: Uint8Array) {
      assert.strictEqual(hexNode(arr), hexBrowser(arr));
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
