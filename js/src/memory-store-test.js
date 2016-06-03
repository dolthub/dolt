// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {assert} from 'chai';
import {suite, test} from 'mocha';
import Chunk from './chunk.js';
import MemoryStore from './memory-store.js';
import Hash from './hash.js';
import {notNull} from './assert.js';

suite('MemoryStore', () => {
  async function assertInputInStore(input: string, hash: Hash, ms: MemoryStore) {
    assert.isTrue(await ms.has(hash));
    const chunk = await ms.get(hash);
    assert.isFalse(chunk.isEmpty());
    assert.strictEqual(input, chunk.toString());
  }

  test('put', async () => {
    const ms = new MemoryStore();
    const input = 'abc';
    let c = Chunk.fromString(input);
    ms.put(c);

    // See http://www.di-mgt.com.au/sha_testvectors.html
    assert.strictEqual('sha1-a9993e364706816aba3e25717850c26c9cd0d89d', c.hash.toString());

    const oldRoot = await ms.getRoot();
    await ms.updateRoot(c.hash, oldRoot);
    await assertInputInStore(input, c.hash, ms);

    // Re-writing the same data should be idempotent and should not result in a second put
    c = Chunk.fromString(input);
    ms.put(c);
    await assertInputInStore(input, c.hash, ms);
  });

  test('updateRoot', async () => {
    const ms = new MemoryStore();
    const oldRoot = await ms.getRoot();
    assert.isTrue(oldRoot.isEmpty());

    // sha1('Bogus, Dude')
    const bogusRoot = notNull(Hash.parse('sha1-81c870618113ba29b6f2b396ea3a69c6f1d626c5'));
     // sha1('Hello, World')
    const newRoot = notNull(Hash.parse('sha1-907d14fb3af2b0d4f18c2d46abe8aedce17367bd'));

    // Try to update root with bogus oldRoot
    let result = await ms.updateRoot(newRoot, bogusRoot);
    assert.isFalse(result);

    // Now do a valid root update
    result = await ms.updateRoot(newRoot, oldRoot);
    assert.isTrue(result);
  });

  test('get non-existing', async () => {
    const ms = new MemoryStore();
    const hash = notNull(Hash.parse('sha1-1111111111111111111111111111111111111111'));
    const c = await ms.get(hash);
    assert.isTrue(c.isEmpty());
  });
});
