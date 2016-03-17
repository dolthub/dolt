// @flow

import {suite, test} from 'mocha';
import {assert} from 'chai';
import Chunk from './chunk.js';
import {deserialize, serialize} from './chunk-serializer.js';

suite('ChunkSerializer', () => {

  function assertChunks(expect: Array<Chunk>, actual: Array<Chunk>) {
    assert.strictEqual(expect.length, actual.length);
    for (let i = 0; i < expect.length; i++) {
      assert.isTrue(expect[i].ref.equals(actual[i].ref));
    }
  }

  test('simple', () => {
    const chunks = [Chunk.fromString('abc'), Chunk.fromString('def'), Chunk.fromString('ghi'),
                    Chunk.fromString('wacka wack wack')];

    const buffer = serialize(chunks);
    const newChunks = deserialize(buffer);

    assertChunks(chunks, newChunks);
  });

  test('leading & trailing empty', () => {
    const chunks = [Chunk.fromString(''), Chunk.fromString('A'), Chunk.fromString('')];

    const buffer = serialize(chunks);
    const newChunks = deserialize(buffer);

    assertChunks(chunks, newChunks);
  });

  test('no chunks', () => {
    const chunks = [];

    const buffer = serialize(chunks);
    const newChunks = deserialize(buffer);

    assertChunks(chunks, newChunks);
  });
});
