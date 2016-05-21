// @flow

import {suite, test} from 'mocha';
import {assert} from 'chai';
import MemoryStore from './memory-store.js';
import BatchStore from './batch-store.js';
import {BatchStoreAdaptorDelegate} from './batch-store-adaptor.js';
import {encodeNomsValue} from './encode.js';

suite('BatchStore', () => {
  test('get after schedulePut works immediately', async () => {
    const ms = new MemoryStore();
    const bs = new BatchStore(3, new BatchStoreAdaptorDelegate(ms));
    const input = 'abc';

    const c = encodeNomsValue(input);
    bs.schedulePut(c, new Set());

    const chunk = await bs.get(c.hash);
    assert.isTrue(c.hash.equals(chunk.hash));
    await bs.close();
  });

  test('get after schedulePut works after flush', async () => {
    const ms = new MemoryStore();
    const bs = new BatchStore(3, new BatchStoreAdaptorDelegate(ms));
    const input = 'abc';

    const c = encodeNomsValue(input);
    bs.schedulePut(c, new Set());

    let chunk = await bs.get(c.hash);
    assert.isTrue(c.hash.equals(chunk.hash));

    await bs.flush();
    chunk = await bs.get(c.hash);
    assert.isTrue(c.hash.equals(chunk.hash));
    await bs.close();
  });
});
