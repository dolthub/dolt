// @flow

import {suite, test} from 'mocha';
import {assert} from 'chai';
import {notNull} from './assert.js';
import OrderedPutCache from './put-cache.js';
import Chunk from './chunk.js';

suite('OrderedPutCache', () => {
  test('append', async () => {
    const canned = [Chunk.fromString('abc'), Chunk.fromString('def')];
    const cache = new OrderedPutCache();
    assert.isTrue(cache.append(canned[0]));
    assert.isTrue(cache.append(canned[1]));
    await cache.destroy();
  });

  test('repeated append returns false', async () => {
    const canned = [Chunk.fromString('abc'), Chunk.fromString('def')];
    const cache = new OrderedPutCache();
    assert.isTrue(cache.append(canned[0]));
    assert.isTrue(cache.append(canned[1]));
    assert.isFalse(cache.append(canned[0]));
    await cache.destroy();
  });

  test('get', async () => {
    const canned = [Chunk.fromString('abc'), Chunk.fromString('def')];
    const cache = new OrderedPutCache();
    assert.isTrue(cache.append(canned[0]));

    let p = cache.get(canned[1].ref.toString());
    assert.isNull(p);

    assert.isTrue(cache.append(canned[1]));
    p = cache.get(canned[1].ref.toString());
    assert.isNotNull(p);
    const chunk = await notNull(p);
    assert.isTrue(canned[1].ref.equals(chunk.ref));

    await cache.destroy();
  });

  test('dropUntil', async () => {
    const canned = [Chunk.fromString('abc'), Chunk.fromString('def'), Chunk.fromString('ghi')];
    const cache = new OrderedPutCache();
    for (const chunk of canned) {
      assert.isTrue(cache.append(chunk));
    }

    await cache.dropUntil(canned[1].ref.toString());

    let p = cache.get(canned[2].ref.toString());
    assert.isNotNull(p);
    const chunk = await notNull(p);
    assert.isTrue(canned[2].ref.equals(chunk.ref));

    p = cache.get(canned[0].ref.toString());
    assert.isNull(p);
    p = cache.get(canned[1].ref.toString());
    assert.isNull(p);

    await cache.destroy();
  });

  test('extractChunks', async () => {
    const canned = [Chunk.fromString('abc'), Chunk.fromString('def'), Chunk.fromString('ghi')];
    const cache = new OrderedPutCache();
    for (const chunk of canned) {
      assert.isTrue(cache.append(chunk));
    }

    const chunkStream = await cache.extractChunks(canned[0].ref.toString(),
      canned[2].ref.toString());
    const chunks = [];
    await chunkStream(chunk => { chunks.push(chunk); });

    for (let i = 0; i < canned.length; i++) {
      assert.isTrue(canned[i].ref.equals(chunks[i].ref));
    }

    await cache.destroy();
  });
});
