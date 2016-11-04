// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {suite, test} from 'mocha';
import {assert} from 'chai';
import MemoryStore from './memory-store.js';
import {BatchStoreAdaptor} from './batch-store.js';
import ValueStore from './value-store.js';
import List from './list.js';
import {encodeValue} from './codec.js';
import {equals} from './compare.js';
import Hash from './hash.js';
import {getHash} from './get-hash.js';
import {notNull} from './assert.js';

suite('ValueStore', () => {
  test('readValue', async () => {
    const ms = new MemoryStore();
    const vs = new ValueStore(new BatchStoreAdaptor(ms));
    const input = 'abc';

    const c = encodeValue(input);
    const v1 = await vs.readValue(c.hash);
    assert.equal(null, v1);

    ms.put(c);
    const v2 = await vs.readValue(c.hash);
    assert.equal('abc', v2);
    await vs.close();
  });

  test('writeValue primitives', async () => {
    const vs = new ValueStore(new BatchStoreAdaptor(new MemoryStore()));

    const r1 = vs.writeValue('hello').targetHash;
    const r2 = vs.writeValue(false).targetHash;
    const r3 = vs.writeValue(2).targetHash;

    const v1 = await vs.readValue(r1);
    assert.equal('hello', v1);
    const v2 = await vs.readValue(r2);
    assert.equal(false, v2);
    const v3 = await vs.readValue(r3);
    assert.equal(2, v3);
    await vs.close();
  });

  test('writeValue rejects invalid', async () => {
    const bs = new BatchStoreAdaptor(new MemoryStore());
    let vs = new ValueStore(bs);
    const r = vs.writeValue('hello');
    vs.flush().then(() => { vs.close(); });

    vs = new ValueStore(bs);
    const l = new List([r]);
    let ex;
    try {
      await vs.writeValue(l);
    } catch (e) {
      ex = e;
    }
    assert.instanceOf(ex, Error);
    await vs.close();
  });

  test('write coalescing', async () => {
    const bs = new BatchStoreAdaptor(new MemoryStore());
    const vs = new ValueStore(bs, 128);

    const r1 = vs.writeValue('hello').targetHash;
    (bs: any).schedulePut = () => { assert.fail('unreachable'); };
    const r2 = vs.writeValue('hello').targetHash;
    assert.isTrue(r1.equals(r2));
    await vs.close();
  });

  test('read caching', async () => {
    const bs = new BatchStoreAdaptor(new MemoryStore());
    const vs = new ValueStore(bs, 128);

    const r1 = vs.writeValue('hello').targetHash;
    const v1 = await vs.readValue(r1);
    assert.equal(v1, 'hello');
    (bs: any).get = () => { throw new Error(); };
    const v2 = await vs.readValue(r1);
    assert.equal(v1, v2);
    await vs.close();
  });

  test('read nonexistince caching', async () => {
    const bs = new BatchStoreAdaptor(new MemoryStore());
    const vs = new ValueStore(bs, 128);

    const hash = notNull(Hash.parse('rmnjb8cjc5tblj21ed4qs821649eduie'));
    const v1 = await vs.readValue(hash);
    assert.equal(null, v1);
    (bs: any).get = () => { throw new Error(); };
    const v2 = await vs.readValue(hash);
    assert.equal(null, v2);
    await vs.close();
  });

  test('write clobbers cached nonexistence', async () => {
    const vs = new ValueStore(new BatchStoreAdaptor(new MemoryStore()), 128);

    const s = 'hello';
    const v1 = await vs.readValue(getHash(s)); // undefined
    assert.equal(null, v1);
    vs.writeValue(s);
    const v2 = await vs.readValue(getHash(s)); // "hello"
    assert.equal(s, v2);
    await vs.close();
  });

  test('caching eviction', async () => {
    const bs = new BatchStoreAdaptor(new MemoryStore());
    const vs = new ValueStore(bs, 15);

    const r1 = vs.writeValue('hello').targetHash;
    const r2 = vs.writeValue('world').targetHash;

    // Prime the cache
    const v1 = await vs.readValue(r1);
    assert.equal(v1, 'hello');
    // Evict v1 from the cache
    const v2 = await vs.readValue(r2);
    assert.equal(v2, 'world');

    (bs: any).get = () => { throw new Error(); };
    let ex;
    try {
      await vs.readValue(r1);
    } catch (e) {
      ex = e;
    }
    assert.instanceOf(ex, Error);
    await vs.close();
  });

  test('hints on cache', async () => {
    const bs = new BatchStoreAdaptor(new MemoryStore());
    const vs = new ValueStore(bs, 15);

    const l = new List([vs.writeValue(1), vs.writeValue(2)]);
    const r = vs.writeValue(l);

    const v = await vs.readValue(r.targetHash);
    assert.isTrue(equals(l, v));
    await vs.close();
  });
});
