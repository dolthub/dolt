// @flow

import {suite, test} from 'mocha';
import {assert} from 'chai';
import Chunk from './chunk.js';
import {deserialize, serialize} from './chunk_serializer.js';

suite('ChunkSerializer', () => {

  function assertChunks(expect: Array<Chunk>, actual: Array<Chunk>) {
    assert.strictEqual(expect.length, actual.length);
    for (let i = 0; i < expect.length; i++) {
      assert.isTrue(expect[i].ref.equals(actual[i].ref));
    }
  }

  test('simple', () => {
    let chunks = [Chunk.fromString('abc'), Chunk.fromString('def'), Chunk.fromString('ghi'),
                  Chunk.fromString('wacka wack wack')];

    let buffer = serialize(chunks);
    let newChunks = deserialize(buffer);

    assertChunks(chunks, newChunks);
  });

  test('leading & trailing empty', () => {
    let chunks = [Chunk.fromString(''), Chunk.fromString('A'), Chunk.fromString('')];

    let buffer = serialize(chunks);
    let newChunks = deserialize(buffer);

    assertChunks(chunks, newChunks);
  });

  test('no chunks', () => {
    let chunks = [];

    let buffer = serialize(chunks);
    let newChunks = deserialize(buffer);

    assertChunks(chunks, newChunks);
  });
});
