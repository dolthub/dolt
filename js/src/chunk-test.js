// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {suite, test} from 'mocha';
import {assert} from 'chai';
import Chunk from './chunk.js';
import Hash from './hash.js';
import {notNull} from './assert.js';

suite('Chunk', () => {
  test('construct', () => {
    const c = Chunk.fromString('abc');
    assert.strictEqual(c.toString(), 'abc');
    assert.isTrue(c.hash.equals(
        notNull(Hash.parse('sha1-a9993e364706816aba3e25717850c26c9cd0d89d'))));
    assert.isFalse(c.isEmpty());
  });

  test('construct with hash', () => {
    const hash = notNull(Hash.parse('sha1-0000000000000000000000000000000000000001'));
    const c = Chunk.fromString('abc', hash);
    assert.strictEqual(c.toString(), 'abc');
    assert.isTrue(c.hash.equals(
        notNull(Hash.parse('sha1-0000000000000000000000000000000000000001'))));
    assert.isFalse(c.isEmpty());
  });

  test('isEmpty', () => {
    function assertChunkIsEmpty(c: Chunk) {
      assert.strictEqual(c.data.length, 0);
      assert.isTrue(c.isEmpty());
    }

    assertChunkIsEmpty(new Chunk(new Uint8Array(0)));
    assertChunkIsEmpty(Chunk.fromString(''));
  });
});
