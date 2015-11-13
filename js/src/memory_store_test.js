/* @flow */

'use strict';

import {assert} from 'chai';
import {suite} from 'mocha';
import Chunk from './chunk.js';
import MemoryStore from './memory_store.js';
import Ref from './ref.js';
import test from './async_test.js';

suite('MemoryStore', () => {
  async function assertInputInStore(input: string, ref: Ref, ms: MemoryStore) {
    assert.isTrue(await ms.has(ref));
    let chunk = await ms.get(ref);
    assert.isFalse(chunk.isEmpty());
    assert.strictEqual(input, chunk.toString());
  }

  test('put', async () => {
    let ms = new MemoryStore();
    let input = 'abc';
    let c = Chunk.fromString(input);
    ms.put(c);

    // See http://www.di-mgt.com.au/sha_testvectors.html
    assert.strictEqual('sha1-a9993e364706816aba3e25717850c26c9cd0d89d', c.ref.toString());

    let oldRoot = await ms.getRoot();
    await ms.updateRoot(c.ref, oldRoot);
    await assertInputInStore(input, c.ref, ms);

    // Re-writing the same data should be idempotent and should not result in a second put
    c = Chunk.fromString(input);
    ms.put(c);
    await assertInputInStore(input, c.ref, ms);
  });

  test('updateRoot', async () => {
    let ms = new MemoryStore();
    let oldRoot = await ms.getRoot();
    assert.isTrue(oldRoot.isEmpty());

    let bogusRoot = Ref.parse('sha1-81c870618113ba29b6f2b396ea3a69c6f1d626c5'); // sha1('Bogus, Dude')
    let newRoot = Ref.parse('sha1-907d14fb3af2b0d4f18c2d46abe8aedce17367bd'); // sha1('Hello, World')

    // Try to update root with bogus oldRoot
    let result = await ms.updateRoot(newRoot, bogusRoot);
    assert.isFalse(result);

    // Now do a valid root update
    result = await ms.updateRoot(newRoot, oldRoot);
    assert.isTrue(result);
  });

  test('get non-existing', async () => {
    let ms = new MemoryStore();
    let ref = Ref.parse('sha1-1111111111111111111111111111111111111111');
    let c = await ms.get(ref);
    assert.isTrue(c.isEmpty());
  });
});
