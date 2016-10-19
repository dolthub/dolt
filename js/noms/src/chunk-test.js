// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {suite, test} from 'mocha';
import {assert} from 'chai';
import Chunk from './chunk.js';
import Hash from './hash.js';
import {notNull} from './assert.js';
import * as Bytes from './bytes.js';

suite('Chunk', () => {
  test('construct', () => {
    const c = Chunk.fromString('abc');
    assert.isTrue(c.hash.equals(
        notNull(Hash.parse('rmnjb8cjc5tblj21ed4qs821649eduie'))));
    assert.isFalse(c.isEmpty());
  });

  test('construct with hash', () => {
    const hash = notNull(Hash.parse('00000000000000000000000000000001'));
    const c = Chunk.fromString('abc', hash);
    assert.isTrue(c.hash.equals(
        notNull(Hash.parse('00000000000000000000000000000001'))));
    assert.isFalse(c.isEmpty());
  });

  test('isEmpty', () => {
    function assertChunkIsEmpty(c: Chunk) {
      assert.strictEqual(c.data.length, 0);
      assert.isTrue(c.isEmpty());
    }

    assertChunkIsEmpty(new Chunk(Bytes.alloc(0)));
    assertChunkIsEmpty(Chunk.fromString(''));
  });
});
