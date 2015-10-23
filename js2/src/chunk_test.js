/* @flow */

'use strict';

const {suite, test} = require('mocha');
const {assert} = require('chai');
const Chunk = require('./chunk.js');
const Ref = require('./ref.js');

suite('Chunk', () => {
  test('construct', () => {
    let c = new Chunk('abc');
    assert.strictEqual(c.data, 'abc');
    assert.isTrue(c.ref.equals(Ref.parse('sha1-a9993e364706816aba3e25717850c26c9cd0d89d')));
    assert.isFalse(c.isEmpty());
  });

  test('construct with ref', () => {
    let ref = Ref.parse('sha1-0000000000000000000000000000000000000001');
    let c = new Chunk('abc', ref);
    assert.strictEqual(c.data, 'abc');
    assert.isTrue(c.ref.equals(Ref.parse('sha1-0000000000000000000000000000000000000001')))
    assert.isFalse(c.isEmpty());
  });

  test('isEmpty', () => {
    function assertChunkIsEmpty(c: Chunk) {
      assert.strictEqual(c.data.length, 0);
      assert.isTrue(c.isEmpty());
    }

    assertChunkIsEmpty(new Chunk());
    assertChunkIsEmpty(new Chunk(''));
  });
});
