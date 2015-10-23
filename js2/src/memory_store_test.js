/* @flow */

'use strict';

const {suite, test} = require('mocha');
const {assert} = require('chai');
const Chunk = require('./chunk.js');
const Ref = require('./ref.js');
const MemoryStore = require('./memory_store.js');

suite('MemoryStore', () => {
  function assertInputInStore(input: string, ref: Ref, ms: MemoryStore) {
    let chunk = ms.get(ref);
    assert.isFalse(chunk.isEmpty());
    assert.strictEqual(input, chunk.data);
  }

  test('put', () => {
    let ms = new MemoryStore();
    let input = 'abc';
    let c = new Chunk(input);
    ms.put(c);

    // See http://www.di-mgt.com.au/sha_testvectors.html
    assert.strictEqual('sha1-a9993e364706816aba3e25717850c26c9cd0d89d', c.ref.toString());

    ms.updateRoot(c.ref, ms.root);

    assertInputInStore(input, c.ref, ms);

    // Re-writing the same data should be idempotent and should not result in a second put
    c = new Chunk(input);
    ms.put(c);
    assertInputInStore(input, c.ref, ms);
  });

  test('updateRoot', () => {
    let ms = new MemoryStore();
    let oldRoot = ms.root;
    assert.isTrue(oldRoot.isEmpty());

    let bogusRoot = Ref.parse('sha1-81c870618113ba29b6f2b396ea3a69c6f1d626c5'); // sha1("Bogus, Dude")
    let newRoot = Ref.parse('sha1-907d14fb3af2b0d4f18c2d46abe8aedce17367bd'); // sha1("Hello, World")

    // Try to update root with bogus oldRoot
    let result = ms.updateRoot(newRoot, bogusRoot);
    assert.isFalse(result);

    // Now do a valid root update
    result = ms.updateRoot(newRoot, oldRoot);
    assert.isTrue(result);
  });

  test('get non-existing', () => {
    let ms = new MemoryStore();
    let ref = Ref.parse('sha1-1111111111111111111111111111111111111111');
    let c = ms.get(ref);
    assert.isTrue(c.isEmpty());
  });
});
