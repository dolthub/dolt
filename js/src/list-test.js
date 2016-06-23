// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {assert} from 'chai';
import {suite, setup, teardown, test} from 'mocha';

import List, {ListWriter, ListLeafSequence} from './list.js';
import Ref from './ref.js';
import {OrderedKey, MetaTuple, newListMetaSequence} from './meta-sequence.js';
import {DEFAULT_MAX_SPLICE_MATRIX_SIZE, calcSplices} from './edit-distance.js';
import {equals} from './compare.js';
import {invariant, notNull} from './assert.js';
import {newStruct} from './struct.js';
import {
  makeListType,
  makeRefType,
  makeUnionType,
  numberType,
  stringType,
} from './type.js';
import {
  assertChunkCountAndType,
  assertValueHash,
  assertValueType,
  deriveCollectionHeight,
  chunkDiffCount,
  flatten,
  flattenParallel,
  intSequence,
  testRoundTripAndValidate,
} from './test-util.js';
import {TestDatabase} from './test-util.js';
import {IndexedMetaSequence} from './meta-sequence.js';

const testListSize = 5000;
const listOfNRef = 'sha1-241e54086d50c131db3c2f3f5f17e68f42fd98ac';

async function assertToJS(list: List, nums: Array<any>, start: number = 0,
    end: number = nums.length): Promise<void> {
  const jsArray = await list.toJS(start, end);
  const expect = nums.slice(start, end);
  assert.deepEqual(expect, jsArray);
}

async function validateList(l: List, values: number[]): Promise<void> {
  assert.isTrue(equals(new List(values), l));
  const out = [];
  await l.forEach(v => void(out.push(v)));
  assert.deepEqual(values, out);
}

// IMPORTANT: These tests and in particular the hash of the values should stay in sync with the
// corresponding tests in go

suite('List', () => {

  function testPrependChunkDiff(nums: Array<any>, list: List, expectCount: number) {
    const nn = new Array(nums.length + 1);
    nn[0] = 0;
    for (let i = 0; i < nums.length; i++) {
      nn[i + 1] = nums[i];
    }

    const v2 = new List(nn);
    assert.strictEqual(expectCount, chunkDiffCount(list, v2));
  }

  function testAppendChunkDiff(nums: Array<any>, list: List, expectCount: number) {
    const nn = new Array(nums.length + 1);
    nn[0] = 0;
    for (let i = 0; i < nums.length; i++) {
      nn[i] = nums[i];
    }
    nn[nums.length] = 0;

    const v2 = new List(nn);
    assert.strictEqual(expectCount, chunkDiffCount(list, v2));
  }

  async function testToJS(expect: Array<any>, list: List): Promise<void> {
    const length = expect.length;
    let start = 0;

    for (let count = Math.round(length / 2); count > 2;) {
      assert.deepEqual(expect.slice(start, start + count), await list.toJS(start, start + count));
      start = start + count;
      count = (length - start) / 2;
    }
  }

  async function testGet(nums: Array<any>, list: List): Promise<void> {
    const incr = Math.round(nums.length / 256); // test 256 indices

    for (let i = 0; i < nums.length; i += incr) {
      assert.strictEqual(nums[i], await list.get(i));
    }
  }

  async function testForEach(nums: Array<any>, list: List): Promise<void> {
    const out = [];
    await list.forEach(v => {
      out.push(v);
    });

    assert.deepEqual(nums, out);
  }

  async function testForEachAsyncCB(nums: Array<any>, list: List): Promise<void> {
    let resolver = null;
    const p = new Promise(resolve => resolver = resolve);

    const out = [];
    const foreachPromise = list.forEach(v => p.then(() => {
      out.push(v);
    }));

    notNull(resolver)();
    return foreachPromise.then(() => assert.deepEqual(nums, out));
  }

  async function listTestSuite(size: number, expectHashStr: string, expectChunkCount: number,
                               expectPrependChunkDiff: number,
                               expectAppendChunkDiff: number): Promise<void> {
    const length = 1 << size;
    const nums = intSequence(length);
    const tr = makeListType(numberType);
    const list = new List(nums);

    assertValueHash(expectHashStr, list);
    assertValueType(tr, list);
    assert.isFalse(list.isEmpty());
    assert.strictEqual(length, list.length);
    assertChunkCountAndType(expectChunkCount, makeRefType(tr), list);

    await testRoundTripAndValidate(list, async(v2) => {
      await assertToJS(v2, nums);
    });

    await testForEach(nums, list);
    await testForEachAsyncCB(nums, list);
    await testToJS(nums, list);
    await testGet(nums, list);
    await testPrependChunkDiff(nums, list, expectPrependChunkDiff);
    await testAppendChunkDiff(nums, list, expectAppendChunkDiff);
  }

  test('List 1K', async () => {
    await listTestSuite(10, 'sha1-99a9e8aa75f9363b561d4576c99630b1103c9083', 2, 2, 2);
  });

  test('LONG: List 4K', async () => {
    await listTestSuite(12, 'sha1-3e4d4c123dceea5b45fcddb207a40b7e0f69a4f2', 4, 2, 2);
  });

  test('LONG: list of ref, set of n numbers, length', async () => {
    const nums = intSequence(testListSize);
    const refs = nums.map(n => new Ref(newStruct('num', {n})));
    const s = new List(refs);
    assert.strictEqual('sha1-ac7830942e248613be6643a2667048667e9c22d1', s.hash.toString());
    assert.strictEqual(testListSize, s.length);

    const height = deriveCollectionHeight(s);
    assert.isTrue(height > 0);
    // height + 1 because the leaves are Ref values (with height 1).
    assert.strictEqual(height + 1, s.sequence.items[0].ref.height);
  });

  async function validateInsertion(values: number[]): Promise<void> {
    let l = new List();
    for (let i = 0; i < values.length; i++) {
      l = await l.insert(i, values[i]);
      await validateList(l, values.slice(0, i + 1));
    }
  }

  test('LONG: validate - insert ascending', async () => {
    await validateInsertion(intSequence(300));
  });

  test('LONG: append', async () => {
    const nums = intSequence(testListSize - 10);
    let s = new List(nums);

    for (let i = testListSize - 10; i < testListSize; i++) {
      s = await s.append(i);
    }

    assert.strictEqual(listOfNRef, s.hash.toString());
  });

  test('LONG: remove', async () => {
    const nums = intSequence(testListSize + 10);
    let s = new List(nums);

    let count = 10;
    while (count-- > 0) {
      s = await s.remove(testListSize + count);
    }

    assert.strictEqual(listOfNRef, s.hash.toString());
  });

  test('LONG: remove at end', async() => {
    const nums = intSequence(testListSize / 20);
    let s = new List(nums);

    for (let i = nums.length - 1; i >= 0; i--) {
      s = await s.remove(i, i + 1);
      const expect = new List(nums.slice(0, i));
      assert.isTrue(equals(expect, s));
    }
  });

  test('LONG: splice', async () => {
    const nums = intSequence(testListSize);
    let s = new List(nums);

    const splice500At = async (idx: number) => {
      s = await s.splice(idx, 500);
      s = await s.splice(idx, 0, ...intSequence(idx + 500, idx));
    };


    for (let i = 0; i < testListSize / 1000; i++) {
      await splice500At(i * 1000);
    }

    assert.strictEqual(listOfNRef, s.hash.toString());
  });

  test('LONG: write, read, modify, read', async () => {
    const db = new TestDatabase();

    const nums = intSequence(testListSize);
    const s = new List(nums);
    const r = db.writeValue(s).targetHash;
    const s2 = await db.readValue(r);
    const outNums = await s2.toJS();
    assert.deepEqual(nums, outNums);

    invariant(s2 instanceof List);
    const s3 = await s2.splice(testListSize - 1, 1);
    const outNums2 = await s3.toJS();
    nums.splice(testListSize - 1, 1);
    assert.deepEqual(nums, outNums2);
    await db.close();
  });
});

suite('ListLeafSequence', () => {
  let db;

  setup(() => {
    db = new TestDatabase();
  });

  teardown((): Promise<void> => db.close());

  test('Empty list isEmpty', () => {
    assert.isTrue(new List().isEmpty());
    assert.isTrue(new List().isEmpty());
  });

  test('iterator', async () => {
    const test = async items => {
      const l = new List(items);
      assert.deepEqual(items, await flatten(l.iterator()));
      assert.deepEqual(items, await flattenParallel(l.iterator(), items.length));
    };

    await test([]);
    await test([42]);
    await test([4, 2, 10, 16]);
  });

  test('iteratorAt', async () => {
    const test = async items => {
      const l = new List(items);
      for (let i = 0; i <= items.length; i++) {
        const slice = items.slice(i);
        assert.deepEqual(slice, await flatten(l.iteratorAt(i)));
        assert.deepEqual(slice, await flattenParallel(l.iteratorAt(i), slice.length));
      }
    };

    await test([]);
    await test([42]);
    await test([4, 2, 10, 16]);
  });
});

suite('CompoundList', () => {
  let db;

  setup(() => {
    db = new TestDatabase();
  });

  teardown((): Promise<void> => db.close());
  function build(): List {
    const l1 = new List(['a', 'b']);
    const r1 = db.writeValue(l1);
    const l2 = new List(['e', 'f']);
    const r2 = db.writeValue(l2);
    const l3 = new List(['h', 'i']);
    const r3 = db.writeValue(l3);
    const l4 = new List(['m', 'n']);
    const r4 = db.writeValue(l4);

    const m1 = List.fromSequence(newListMetaSequence(db, [
      new MetaTuple(r1, new OrderedKey(2), 2, null),
      new MetaTuple(r2, new OrderedKey(2), 2, null)]));
    const rm1 = db.writeValue(m1);
    const m2 = List.fromSequence(newListMetaSequence(db, [
      new MetaTuple(r3, new OrderedKey(2), 2, null),
      new MetaTuple(r4, new OrderedKey(2), 2, null)]));
    const rm2 = db.writeValue(m2);

    const l = List.fromSequence(newListMetaSequence(db, [
      new MetaTuple(rm1, new OrderedKey(4), 4, null),
      new MetaTuple(rm2, new OrderedKey(4), 4, null)]));
    return l;
  }

  test('iterator', async () => {
    const l = build();
    const expected = ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n'];
    assert.deepEqual(expected, await flatten(l.iterator()));
    assert.deepEqual(expected, await flattenParallel(l.iterator(), expected.length));
  });

  test('iteratorAt', async () => {
    const values = ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n'];
    for (let i = 0; i <= values.length; i++) {
      const l = build();
      const slice = values.slice(i);
      assert.deepEqual(slice, await flatten(l.iteratorAt(i)));
      assert.deepEqual(slice, await flattenParallel(l.iteratorAt(i), slice.length));
    }
  });

  test('iterator return', async () => {
    const list = build();
    const iter = list.iterator();
    const values = [];
    for (let res = await iter.next(); !res.done; res = await iter.next()) {
      values.push(res.value);
      if (values.length === 5) {
        await iter.return();
      }
    }
    assert.deepEqual(values, ['a', 'b', 'e', 'f', 'h']);
  });

  test('iterator return parallel', async () => {
    const list = build();
    const iter = list.iterator();
    const values = await Promise.all([iter.next(), iter.next(), iter.return(), iter.next()]);
    assert.deepEqual(
        [{done: false, value: 'a'}, {done: false, value: 'b'}, {done: true}, {done: true}],
        values);
  });
});

suite('Diff List', () => {
  test('Identical', async () => {
    const nums1 = intSequence(5);
    const nums2 = nums1.slice(0);

    const directDiff = calcSplices(nums1.length, nums2.length, DEFAULT_MAX_SPLICE_MATRIX_SIZE,
      (i, j) => nums1[i] === nums2[j]);

    const l1 = new List(nums1);
    const l2 = new List(nums2);

    const listDiff = await l2.diff(l1);
    assert.deepEqual(directDiff, listDiff);

    const expectedDiff = [];
    assert.deepEqual(expectedDiff, directDiff);
  });

  test('LONG: Remove 5x100', async () => {
    const nums1 = intSequence(5000);
    const nums2 = nums1.slice(0);

    let count = 5;
    while (count-- > 0) {
      nums2.splice(count * 1000, 100);
    }

    const directDiff = calcSplices(nums1.length, nums2.length, DEFAULT_MAX_SPLICE_MATRIX_SIZE,
      (i, j) => nums1[i] === nums2[j]);

    const l1 = new List(nums1);
    const l2 = new List(nums2);

    const listDiff = await l2.diff(l1);
    assert.deepEqual(directDiff, listDiff);

    const expectedDiff = [
      [0, 100, 0, 0],
      [1000, 100, 0, 0],
      [2000, 100, 0, 0],
      [3000, 100, 0, 0],
      [4000, 100, 0, 0],
    ];
    assert.deepEqual(expectedDiff, directDiff);
  });

  test('LONG: Reverse', async () => {
    const nums1 = intSequence(5000);
    const nums2 = nums1.slice(0).reverse();

    const directDiff = calcSplices(nums1.length, nums2.length, DEFAULT_MAX_SPLICE_MATRIX_SIZE,
      (i, j) => nums1[i] === nums2[j]);

    const l1 = new List(nums1);
    const l2 = new List(nums2);

    const listDiff = await l2.diff(l1);
    assert.deepEqual(directDiff, listDiff);

    const expectedDiff = [[0, 5000, 5000, 0]];
    assert.deepEqual(expectedDiff, directDiff);
  });

  test('LONG: Reverse - Larger Limit', async () => {
    const nums1 = intSequence(5000);
    const nums2 = nums1.slice(0).reverse();

    const directDiff = calcSplices(nums1.length, nums2.length, 27e6,
      (i, j) => nums1[i] === nums2[j]);

    const l1 = new List(nums1);
    const l2 = new List(nums2);

    const listDiff = await l2.diff(l1, 27e6);
    assert.deepEqual(directDiff, listDiff);

    const expectedDiff = [
      [0, 2499, 2500, 0],
      [2500, 2500, 2499, 2501],
    ];
    assert.deepEqual(expectedDiff, directDiff);
  });

  test('LONG: Add 5x5', async () => {
    const nums1 = intSequence(5000);
    const nums2 = nums1.slice(0);

    let count = 5;
    while (count-- > 0) {
      nums2.splice(count * 1000, 0, 0, 1, 2, 3, 4);
    }

    const directDiff = calcSplices(nums1.length, nums2.length, DEFAULT_MAX_SPLICE_MATRIX_SIZE,
      (i, j) => nums1[i] === nums2[j]);

    const l1 = new List(nums1);
    const l2 = new List(nums2);

    const listDiff = await l2.diff(l1);
    assert.deepEqual(directDiff, listDiff);

    const expectedDiff = [
      [5, 0, 5, 5],
      [1000, 0, 5, 1005],
      [2000, 0, 5, 2010],
      [3000, 0, 5, 3015],
      [4000, 0, 5, 4020],
    ];
    assert.deepEqual(expectedDiff, directDiff);
  });

  test('LONG: Replace reverse 5x100', async () => {
    const nums1 = intSequence(5000);
    const nums2 = nums1.slice(0);

    let count = 5;
    while (count-- > 0) {
      const out = nums2.slice(count * 1000, count * 1000 + 100).reverse();
      nums2.splice(count * 1000, 100, ...out);
    }

    const directDiff = calcSplices(nums1.length, nums2.length, DEFAULT_MAX_SPLICE_MATRIX_SIZE,
      (i, j) => nums1[i] === nums2[j]);
    const l1 = new List(nums1);
    const l2 = new List(nums2);

    const listDiff = await l2.diff(l1);
    assert.deepEqual(directDiff, listDiff);

    const expectedDiff = [
      [0,49,50,0],
      [50,50,49,51],
      [1000,49,50,1000],
      [1050,50,49,1051],
      [2000,49,50,2000],
      [2050,50,49,2051],
      [3000,49,50,3000],
      [3050,50,49,3051],
      [4000,49,50,4000],
      [4050,50,49,4051],
    ];
    assert.deepEqual(expectedDiff, directDiff);
  });

  test('String 1', async () => {
    const nums1 = ['one', 'two', 'three'];
    const nums2 = nums1.slice(0);

    const directDiff = calcSplices(nums1.length, nums2.length, DEFAULT_MAX_SPLICE_MATRIX_SIZE,
      (i, j) => equals(nums1[i],nums2[j]));
    const l1 = new List(nums1);
    const l2 = new List(nums2);

    const listDiff = await l2.diff(l1);
    assert.deepEqual(directDiff, listDiff);

    const expectedDiff = [];
    assert.deepEqual(expectedDiff, directDiff);
  });

  test('String 2', async () => {
    const nums1 = ['one', 'two', 'three'];
    const nums2 = ['one', 'two', 'three', 'four'];

    const directDiff = calcSplices(nums1.length, nums2.length, DEFAULT_MAX_SPLICE_MATRIX_SIZE,
      (i, j) => equals(nums1[i],nums2[j]));
    const l1 = new List(nums1);
    const l2 = new List(nums2);

    const listDiff = await l2.diff(l1);
    assert.deepEqual(directDiff, listDiff);

    const expectedDiff = [
      [3,0,1,3],
    ];
    assert.deepEqual(expectedDiff, directDiff);
  });

  test('String 3', async () => {
    const nums1 = ['one', 'two', 'three'];
    const nums2 = ['one', 'two', 'four'];

    const directDiff = calcSplices(nums1.length, nums2.length, DEFAULT_MAX_SPLICE_MATRIX_SIZE,
      (i, j) => equals(nums1[i],nums2[j]));
    const l1 = new List(nums1);
    const l2 = new List(nums2);

    const listDiff = await l2.diff(l1);
    assert.deepEqual(directDiff, listDiff);

    const expectedDiff = [
      [2,1,1,2],
    ];
    assert.deepEqual(expectedDiff, directDiff);
  });

});

suite('ListWriter', () => {
  let db;

  setup(() => {
    db = new TestDatabase();
  });

  teardown((): Promise<void> => db.close());

  test('ListWriter', async () => {
    const values = intSequence(15);
    const l = new List(values);

    const w = new ListWriter();
    for (let i = 0; i < values.length; i++) {
      w.write(values[i]);
    }

    w.close();
    const l2 = w.list;
    const l3 = w.list;
    assert.isTrue(equals(l, l2));
    assert.strictEqual(l2, l3);
  });

  test('ListWriter close throws', () => {
    const values = intSequence(15);
    const w = new ListWriter();
    for (let i = 0; i < values.length; i++) {
      w.write(values[i]);
    }
    w.close();

    let ex;
    try {
      w.close();  // Cannot close twice.
    } catch (e) {
      ex = e;
    }
    assert.instanceOf(ex, TypeError);
  });

  test('ListWriter write throws', () => {
    const values = intSequence(15);
    const w = new ListWriter();
    for (let i = 0; i < values.length; i++) {
      w.write(values[i]);
    }
    w.close();

    let ex;
    try {
      w.write(42);  // Cannot write after close.
    } catch (e) {
      ex = e;
    }
    assert.instanceOf(ex, TypeError);
  });

  test('ListWriter with ValueReadWriter', async () => {
    const values = intSequence(75);
    const l = new List(values);

    // The number of writes depends on how many chunks we've encountered.
    let writes = 0;
    assert.equal(db.writeCount, writes);

    const w = new ListWriter(db);
    for (let i = 0; i < values.length; i++) {
      w.write(values[i]);
    }

    writes++;
    assert.equal(db.writeCount, writes);

    w.close();
    writes += 2;  // one for the last leaf chunk and one for the meta chunk.
    assert.equal(db.writeCount, writes);

    const l2 = w.list;
    const l3 = w.list;
    assert.isTrue(equals(l, l2));
    assert.strictEqual(l2, l3);
  });

  test('Type after mutations', async () => {
    async function t(n, c) {
      const values: any = intSequence(n);

      let l = new List(values);
      assert.equal(l.length, n);
      assert.instanceOf(l.sequence, c);
      assert.isTrue(equals(l.type, makeListType(numberType)));

      l = await l.append('a');
      assert.equal(l.length, n + 1);
      assert.instanceOf(l.sequence, c);
      assert.isTrue(equals(l.type, makeListType(makeUnionType([numberType, stringType]))));

      l = await l.splice(l.length - 1, 1);
      assert.equal(l.length, n);
      assert.instanceOf(l.sequence, c);
      assert.isTrue(equals(l.type, makeListType(numberType)));
    }

    await t(10, ListLeafSequence);
    await t(100, IndexedMetaSequence);
  });
});
