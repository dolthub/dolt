// @flow

import {suite, test} from 'mocha';
import {assert} from 'chai';
import MemoryStore from './memory-store.js';
import type {ChunkStore} from './chunk-store.js';
import BatchStore from './batch-store.js';
import {BatchStoreAdaptorDelegate} from './batch-store-adaptor.js';
import ValueStore from './value-store.js';
import {newList} from './list.js';
import {encodeNomsValue} from './encode.js';
import {equals} from './compare.js';

export class FakeBatchStore extends BatchStore {
  constructor(cs: ChunkStore) {
    super(3, new BatchStoreAdaptorDelegate(cs));
  }
}

suite('ValueStore', () => {
  test('readValue', async () => {
    const ms = new MemoryStore();
    const vs = new ValueStore(new FakeBatchStore(ms));
    const input = 'abc';

    const c = encodeNomsValue(input);
    const v1 = await vs.readValue(c.ref);
    assert.equal(null, v1);

    ms.put(c);
    const v2 = await vs.readValue(c.ref);
    assert.equal('abc', v2);
  });

  test('writeValue primitives', async () => {
    const vs = new ValueStore(new FakeBatchStore(new MemoryStore()));

    const r1 = vs.writeValue('hello').targetRef;
    const r2 = vs.writeValue(false).targetRef;
    const r3 = vs.writeValue(2).targetRef;

    const v1 = await vs.readValue(r1);
    assert.equal('hello', v1);
    const v2 = await vs.readValue(r2);
    assert.equal(false, v2);
    const v3 = await vs.readValue(r3);
    assert.equal(2, v3);
  });

  test('writeValue rejects invalid', async () => {
    const bs = new FakeBatchStore(new MemoryStore());
    let vs = new ValueStore(bs);
    const r = vs.writeValue('hello');
    vs.flush().then(() => { vs.close(); });

    vs = new ValueStore(bs);
    const l = await newList([r]);
    let ex;
    try {
      await vs.writeValue(l);
    } catch (e) {
      ex = e;
    }
    assert.instanceOf(ex, Error);
  });

  test('write coalescing', async () => {
    const bs = new FakeBatchStore(new MemoryStore());
    const vs = new ValueStore(bs, 1e6);

    const r1 = vs.writeValue('hello').targetRef;
    (bs: any).schedulePut = () => { assert.fail('unreachable'); };
    const r2 = vs.writeValue('hello').targetRef;
    assert.isTrue(r1.equals(r2));
  });

  test('read caching', async () => {
    const bs = new FakeBatchStore(new MemoryStore());
    const vs = new ValueStore(bs, 1e6);

    const r1 = vs.writeValue('hello').targetRef;
    const v1 = await vs.readValue(r1);
    assert.equal(v1, 'hello');
    (bs: any).get = () => { throw new Error(); };
    const v2 = await vs.readValue(r1);
    assert.equal(v1, v2);
  });

  test('caching eviction', async () => {
    const bs = new FakeBatchStore(new MemoryStore());
    const vs = new ValueStore(bs, 15);

    const r1 = vs.writeValue('hello').targetRef;
    const r2 = vs.writeValue('world').targetRef;

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
  });

  test('hints on cache', async () => {
    const bs = new FakeBatchStore(new MemoryStore());
    const vs = new ValueStore(bs, 15);

    const l = await newList([vs.writeValue(1), vs.writeValue(2)]);
    const r = vs.writeValue(l);
    // await vs.flush();

    const v = await vs.readValue(r.targetRef);
    assert.isTrue(equals(l, v));
  });
});
