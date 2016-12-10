// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {suite, suiteSetup, suiteTeardown, test} from 'mocha';
import {assert} from 'chai';

import {BatchStoreAdaptor} from './batch-store.js';
import {createStructClass} from './struct.js';
import Database from './database.js';
import {
  makeListType,
  makeStructType,
  numberType,
  stringType,
} from './type.js';
import MemoryStore from './memory-store.js';
import Blob from './blob.js';
import List from './list.js';
import Map from './map.js';
import NomsSet from './set.js'; // namespace collision with JS Set
import walk from './walk.js';
import type Value from './value.js';
import {smallTestChunks, normalProductionChunks} from './rolling-value-hasher.js';

suite('walk', () => {
  let ds;
  suiteSetup(() => {
    smallTestChunks();
    ds = new Database(new BatchStoreAdaptor(new MemoryStore()));
  });

  suiteTeardown((): Promise<void> => {
    normalProductionChunks();
    return ds.close();
  });

  test('primitives', async () => {
    await Promise.all([true, false, 42, 88.8, 'hello!', ''].map(async v => {
      await callbackHappensOnce(v, ds, false);
    }));
  });

  test('blob', async () => {
    const arr = new Uint32Array(1000);
    for (let i = 0; i < arr.length; i++) {
      arr[i] = i;
    }
    const blob = new Blob(new Uint8Array(arr.buffer));
    assert.equal(blob.length, arr.length * 4);
    assert.isAbove(blob.chunks.length, 1);

    await callbackHappensOnce(blob, ds, false);
  });

  test('list', async () => {
    const expected = new Set();
    for (let i = 0; i < 1000; i++) {
      expected.add(i);
    }
    const list = new List(Array.from(expected));
    expected.add(list);

    await callbackHappensOnce(list, ds, true);

    await walk(list, ds, async v => {
      assert.isOk(expected.delete(v));
    });
    assert.equal(0, expected.size);
  });

  test('set', async () => {
    const expected = new Set();
    for (let i = 0; i < 1000; i++) {
      expected.add(String(i));
    }
    const set = new NomsSet(Array.from(expected));
    expected.add(set);

    await callbackHappensOnce(set, ds, true);

    await walk(set, ds, async v => {
      assert.isOk(expected.delete(v));
    });
    assert.equal(0, expected.size);
  });

  test('map', async () => {
    const expected = [];
    const entries = [];
    for (let i = 0; i < 1000; i++) {
      expected.push(i);
      expected.push('value' + i);
      entries.push([i, 'value' + i]);
    }
    const map = new Map(entries);
    expected.push(map);

    await callbackHappensOnce(map, ds, true);

    await walk(map, ds, async v => {
      const idx = expected.indexOf(v);
      assert.isAbove(idx, -1);
      assert.equal(expected.splice(idx, 1).length, 1);
    });
    assert.equal(0, expected.length);
  });

  test('struct', async () => {
    const t = makeStructType('Thing', {
      foo: stringType,
      list: makeListType(numberType),
      num: numberType,
    });

    const c = createStructClass(t);
    const val = new c({
      foo: 'bar',
      num: 42,
      list: new List([1, 2]),
    });

    await callbackHappensOnce(val, ds, true);

    const expected = new Set([val, val.foo, val.num, val.list, 1, 2]);
    await walk(val, ds, async v => {
      assert.isOk(expected.delete(v));
    });
    assert.equal(0, expected.size);
  });

  test('ref-value', async () => {
    const rv = ds.writeValue(42);
    const expected = new Set([rv, 42]);
    await callbackHappensOnce(rv, ds, true);
    await walk(rv, ds, async v => {
      assert.isOk(expected.delete(v));
    });
    assert.equal(0, expected.size);
  });

  test('cb-should-recurse', async () => {
    const testShouldRecurse = async (cb, expectSkip) => {
      const rv = ds.writeValue(42);
      const expected = new Set([rv, 42]);
      await walk(rv, ds, v => {
        assert.isOk(expected.delete(v));
        return cb();
      });
      assert.equal(expectSkip ? 1 : 0, expected.size);
    };

    // Return void, Promise<void>, false, or Promise<false> -- should recurse.
    await testShouldRecurse(() => { return; }, false); // eslint-disable-line
    await testShouldRecurse(() => Promise.resolve(), false);
    await testShouldRecurse(() => false, false);
    await testShouldRecurse(() => Promise.resolve(false), false);

    // Return true or Promise<true> -- should skip
    await testShouldRecurse(() => true, true);
    await testShouldRecurse(() => Promise.resolve(true), true);
  });
});

async function callbackHappensOnce(v: Value, ds: Database, skip: boolean): Promise<void> {
  // Test that our callback only gets called once.
  let count = 0;
  await walk(v, ds, async cv => {
    assert.strictEqual(v, cv);
    count++;
    return skip;
  });
  assert.equal(1, count);
}
