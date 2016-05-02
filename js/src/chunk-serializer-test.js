// @flow

import {suite, test} from 'mocha';
import {assert} from 'chai';
import Chunk from './chunk.js';
import Ref from './ref.js';
import {deserialize, serialize} from './chunk-serializer.js';

suite('ChunkSerializer', () => {

  function assertHints(expect: Array<Ref>, actual: Array<Ref>) {
    assert.strictEqual(actual.length, expect.length);
    for (let i = 0; i < expect.length; i++) {
      assert.isTrue(expect[i].equals(actual[i]));
    }
  }

  function assertChunks(expect: Array<Chunk>, actual: Array<Chunk>) {
    assert.strictEqual(actual.length, expect.length);
    for (let i = 0; i < expect.length; i++) {
      assert.isTrue(expect[i].ref.equals(actual[i].ref));
    }
  }

  test('simple', () => {
    const expHints = [];
    const expChunks = [Chunk.fromString('abc'), Chunk.fromString('def'), Chunk.fromString('ghi'),
                       Chunk.fromString('wacka wack wack')];

    const {hints, chunks} = deserialize(serialize(new Set(expHints), expChunks));

    assertHints(expHints, hints);
    assertChunks(expChunks, chunks);
  });

  test('leading & trailing empty', () => {
    const expHints = [];
    const expChunks = [Chunk.fromString(''), Chunk.fromString('A'), Chunk.fromString('')];

    const {hints, chunks} = deserialize(serialize(new Set(expHints), expChunks));

    assertHints(expHints, hints);
    assertChunks(expChunks, chunks);
  });

  test('all empty', () => {
    const expHints = [];
    const expChunks = [];

    const {hints, chunks} = deserialize(serialize(new Set(expHints), expChunks));

    assertHints(expHints, hints);
    assertChunks(expChunks, chunks);
  });

  test('with hints', () => {
    const expHints = [
      Chunk.fromString('123').ref,
      Chunk.fromString('456').ref,
      Chunk.fromString('789').ref,
      Chunk.fromString('wacka wack wack').ref,
    ];
    const expChunks = [Chunk.fromString('abc'), Chunk.fromString('def'), Chunk.fromString('ghi')];

    const {hints, chunks} = deserialize(serialize(new Set(expHints), expChunks));

    assertHints(expHints, hints);
    assertChunks(expChunks, chunks);
  });
});
