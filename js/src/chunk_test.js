// @flow

import {suite, test} from 'mocha';
import {assert} from 'chai';
import Chunk from './chunk.js';
import Ref from './ref.js';

suite('Chunk', () => {
  test('construct', () => {
    const c = Chunk.fromString('abc');
    assert.strictEqual(c.toString(), 'abc');
    assert.isTrue(c.ref.equals(Ref.parse('sha1-a9993e364706816aba3e25717850c26c9cd0d89d')));
    assert.isFalse(c.isEmpty());
  });

  test('construct with ref', () => {
    const ref = Ref.parse('sha1-0000000000000000000000000000000000000001');
    const c = Chunk.fromString('abc', ref);
    assert.strictEqual(c.toString(), 'abc');
    assert.isTrue(c.ref.equals(Ref.parse('sha1-0000000000000000000000000000000000000001')));
    assert.isFalse(c.isEmpty());
  });

  test('isEmpty', () => {
    function assertChunkIsEmpty(c: Chunk) {
      assert.strictEqual(c.data.length, 0);
      assert.isTrue(c.isEmpty());
    }

    assertChunkIsEmpty(new Chunk());
    assertChunkIsEmpty(Chunk.fromString(''));
  });
});
