// @flow

import BatchStoreAdaptor from './batch-store-adaptor.js';
import {createStructClass} from './struct.js';
import DataStore from './data-store.js';
import {
  makeListType,
  makeMapType,
  makeSetType,
  makeStructType,
  numberType,
  stringType,
} from './type.js';
import MemoryStore from './memory-store.js';
import {newBlob} from './blob.js';
import {newList} from './list.js';
import {newMap} from './map.js';
import {newSet} from './set.js';
import walk from './walk.js';

import {suite, test} from 'mocha';
import {assert} from 'chai';

import type {valueOrPrimitive} from './value.js';

suite('walk', () => {
  const ds = new DataStore(new BatchStoreAdaptor(new MemoryStore()));

  test('primitives', async () => {
    await Promise.all([true, false, 42, 88.8, 'hello!', ''].map(async v => {
      await callbackHappensOnce(v, ds, true);
    }));
  });

  test('blob', async () => {
    const arr = new Uint32Array(1000);
    for (let i = 0; i < arr.length; i++) {
      arr[i] = i;
    }
    const blob = await newBlob(new Uint8Array(arr.buffer));
    assert.equal(blob.length, arr.length * 4);
    assert.isAbove(blob.chunks.length, 1);

    await callbackHappensOnce(blob, ds, true);
  });

  test('list', async () => {
    const expected = new Set();
    for (let i = 0; i < 1000; i++) {
      expected.add(i);
    }
    const list = await newList(Array.from(expected), makeListType(numberType));
    expected.add(list);

    await callbackHappensOnce(list, ds);

    await walk(list, ds, async v => {
      assert.isOk(expected.delete(v));
      return true;
    });
    assert.equal(0, expected.size);
  });

  test('set', async () => {
    const expected = new Set();
    for (let i = 0; i < 1000; i++) {
      expected.add(String(i));
    }
    const set = await newSet(Array.from(expected), makeSetType(stringType));
    expected.add(set);

    await callbackHappensOnce(set, ds);

    await walk(set, ds, async v => {
      assert.isOk(expected.delete(v));
      return true;
    });
    assert.equal(0, expected.size);
  });

  test('map', async () => {
    const expected = [];
    for (let i = 0; i < 1000; i++) {
      expected.push(i);
      expected.push(String('value' + i));
    }
    const map = await newMap(Array.from(expected), makeMapType(numberType, stringType));
    expected.push(map);

    await callbackHappensOnce(map, ds);

    await walk(map, ds, async v => {
      const idx = expected.indexOf(v);
      assert.isAbove(idx, -1);
      assert.equal(expected.splice(idx, 1).length, 1);
      return true;
    });
    assert.equal(0, expected.length);
  });

  test('struct', async () => {
    const t = makeStructType('Thing', {
      foo: stringType,
      num: numberType,
      list: makeListType(numberType),
    });
    const c = createStructClass(t);
    const val = new c({
      foo: 'bar',
      num: 42,
      list: await newList([1,2], makeListType(numberType)),
    });

    await callbackHappensOnce(val, ds);

    const expected = new Set([val, val.foo, val.num, val.list, 1, 2]);
    await walk(val, ds, async v => {
      assert.isOk(expected.delete(v));
      return true;
    });
    assert.equal(0, expected.size);
  });

  test('ref-value', async () => {
    const rv = ds.writeValue(42);
    const expected = new Set([rv, 42]);
    await callbackHappensOnce(rv, ds);
    await walk(rv, ds, async v => {
      assert.isOk(expected.delete(v));
      return true;
    });
    assert.equal(0, expected.size);
  });

  test('cb-default', async () => {
    const rv = ds.writeValue(42);
    const expected = new Set([rv, 42]);
    await walk(rv, ds, async v => {
      assert.isOk(expected.delete(v));
      // return nothing -- default should be to recurse.
    });
    assert.equal(0, expected.size);
  });
});

async function callbackHappensOnce(v: valueOrPrimitive, ds: DataStore,
                                   recurse: bool = false) : Promise<void> {
  // Test that our callback only gets called once.
  let count = 0;
  await walk(v, ds, async cv => {
    assert.strictEqual(v, cv);
    count++;
    return recurse;
  });
  assert.equal(1, count);
}
