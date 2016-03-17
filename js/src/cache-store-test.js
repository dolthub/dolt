// @flow

import {assert} from 'chai';
import {suite} from 'mocha';
import Chunk from './chunk.js';
import CacheStore from './cache-store.js';
import MemoryStore from './memory-store.js';
import Ref from './ref.js';
import test from './async-test.js';
import {TextEncoder} from './text-encoding.js';

suite('CacheStore', () => {

  function ref(s) {
    return Ref.fromData(new TextEncoder().encode(s));
  }

  test('has', async () => {
    const ms = new MemoryStore();
    ms.put(Chunk.fromString('a'));
    ms.put(Chunk.fromString('b'));
    ms.put(Chunk.fromString('c'));

    const cs = new CacheStore(ms, 5);
    cs.put(Chunk.fromString('d'));
    cs.put(Chunk.fromString('e'));
    cs.put(Chunk.fromString('f'));

    assert.isTrue(await cs.has(ref('a')));
    assert.equal(cs._hasCache.size, 1);
    assert.isTrue(await cs.has(ref('b')));
    assert.equal(cs._hasCache.size, 2);

    assert.isFalse(await cs.has(ref('x')));
    assert.equal(cs._hasCache.size, 2);

    assert.isTrue(await cs.has(ref('c')));
    assert.isTrue(await cs.has(ref('d')));
    assert.isTrue(await cs.has(ref('e')));
    assert.equal(cs._hasCache.size, 5);
    assert.isTrue(await cs.has(ref('f')));
    assert.equal(cs._hasCache.size, 5);
  });

  test('get', async () => {
    const ms = new MemoryStore();

    const chunkA = Chunk.fromString('a');
    const chunkB = Chunk.fromString('b');
    const chunkC = Chunk.fromString('c');
    const chunkD = Chunk.fromString('d');
    const chunkE = Chunk.fromString('e');
    const chunkF = Chunk.fromString('f');

    ms.put(chunkA);
    ms.put(chunkB);
    ms.put(chunkC);

    const cs = new CacheStore(ms, 5);
    cs.put(chunkD);
    cs.put(chunkE);
    cs.put(chunkF);

    assert.deepEqual(await cs.get(ref('a')), chunkA);
    assert.equal(cs._chunkCache.size, 1);
    assert.deepEqual(await cs.get(ref('b')), chunkB);
    assert.equal(cs._chunkCache.size, 2);

    assert.isTrue((await cs.get(ref('x'))).isEmpty());
    assert.equal(cs._chunkCache.size, 2);

    assert.deepEqual(await cs.get(ref('c')), chunkC);
    assert.deepEqual(await cs.get(ref('d')), chunkD);
    assert.deepEqual(await cs.get(ref('e')), chunkE);
    assert.equal(cs._chunkCache.size, 5);
    assert.deepEqual(await cs.get(ref('f')), chunkF);
    assert.equal(cs._chunkCache.size, 5);
  });

});
