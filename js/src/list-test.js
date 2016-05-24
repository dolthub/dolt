// @flow

import {assert} from 'chai';
import {suite, setup, teardown, test} from 'mocha';

import Database from './database.js';
import {makeTestingBatchStore} from './batch-store-adaptor.js';
import Ref from './ref.js';
import {newStruct} from './struct.js';
import {calcSplices} from './edit-distance.js';
import {
  makeRefType,
  makeListType,
  numberType,
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
import {MetaTuple, newListMetaSequence} from './meta-sequence.js';
import {invariant} from './assert.js';
import List, {newListFromSequence} from './list.js';

const testListSize = 5000;
const listOfNRef = 'sha1-aa1605484d993e89dbc0431acb9f2478282f9d94';

async function assertToJS(list: List, nums: Array<any>, start: number = 0,
    end: number = nums.length): Promise<void> {
  const jsArray = await list.toJS(start, end);
  const expect = nums.slice(start, end);
  assert.deepEqual(expect, jsArray);
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
    await testToJS(nums, list);
    await testGet(nums, list);
    await testPrependChunkDiff(nums, list, expectPrependChunkDiff);
    await testAppendChunkDiff(nums, list, expectAppendChunkDiff);
  }

  test('List 1K', async () => {
    await listTestSuite(10, 'sha1-26169e4d8d3175994c992ca21be07c30ad2007e3', 17, 19, 2);
  });

  test('LONG: List 4K', async () => {
    await listTestSuite(12, 'sha1-35f79a6d2ddbe34ad469b1bf2a9a1b460e0e997c', 2, 3, 2);
  });

  test('LONG: list of ref, set of n numbers, length', async () => {
    const nums = intSequence(testListSize);
    const refs = nums.map(n => new Ref(newStruct('num', {n})));
    const s = new List(refs);
    assert.strictEqual(s.hash.toString(), 'sha1-2e79d54322aa793d0e8d48380a28927a257a141a');
    assert.strictEqual(testListSize, s.length);

    const height = deriveCollectionHeight(s);
    assert.isTrue(height > 0);
    // height + 1 because the leaves are Ref values (with height 1).
    assert.strictEqual(height + 1, s.sequence.items[0].ref.height);
  });

  test('LONG: insert', async () => {
    const nums = intSequence(testListSize - 10);
    let s = new List(nums);

    for (let i = testListSize - 10; i < testListSize; i++) {
      s = await s.insert(i, i);
    }

    assert.strictEqual(s.hash.toString(), listOfNRef);
  });

  test('LONG: append', async () => {
    const nums = intSequence(testListSize - 10);
    let s = new List(nums);

    for (let i = testListSize - 10; i < testListSize; i++) {
      s = await s.append(i);
    }

    assert.strictEqual(s.hash.toString(), listOfNRef);
  });

  test('LONG: remove', async () => {
    const nums = intSequence(testListSize + 10);
    let s = new List(nums);

    let count = 10;
    while (count-- > 0) {
      s = await s.remove(testListSize + count, testListSize + count + 1);
    }

    assert.strictEqual(s.hash.toString(), listOfNRef);
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

    assert.strictEqual(s.hash.toString(), listOfNRef);
  });

  test('LONG: write, read, modify, read', async () => {
    const db = new Database(makeTestingBatchStore());

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
    db = new Database(makeTestingBatchStore());
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
    db = new Database(makeTestingBatchStore());
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

    const m1 = newListFromSequence(newListMetaSequence(
      db, [new MetaTuple(r1, 2, 2), new MetaTuple(r2, 2, 2)]));
    const rm1 = db.writeValue(m1);
    const m2 = newListFromSequence(newListMetaSequence(
      db, [new MetaTuple(r3, 2, 2), new MetaTuple(r4, 2, 2)]));
    const rm2 = db.writeValue(m2);

    const l = newListFromSequence(newListMetaSequence(
      db, [new MetaTuple(rm1, 4, 4), new MetaTuple(rm2, 4, 4)]));
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
  test('LONG: Remove 5x100', async () => {
    const nums1 = intSequence(5000);
    const nums2 = nums1.slice(0);

    let count = 5;
    while (count-- > 0) {
      nums2.splice(count * 1000, 100);
    }

    const directDiff = calcSplices(nums1.length, nums2.length, (i, j) => nums1[i] === nums2[j]);

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

  test('LONG: Add 5x5', async () => {
    const nums1 = intSequence(5000);
    const nums2 = nums1.slice(0);

    let count = 5;
    while (count-- > 0) {
      nums2.splice(count * 1000, 0, 0, 1, 2, 3, 4);
    }

    const directDiff = calcSplices(nums1.length, nums2.length, (i, j) => nums1[i] === nums2[j]);

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

    const directDiff = calcSplices(nums1.length, nums2.length, (i, j) => nums1[i] === nums2[j]);
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

  test('LONG: Load Limit', async () => {
    const nums1 = intSequence(5);
    const nums2 = intSequence(5000);

    const directDiff = calcSplices(nums1.length, nums2.length, (i, j) => nums1[i] === nums2[j]);
    const l1 = new List(nums1);
    const l2 = new List(nums2);

    const listDiff = await l2.diff(l1);
    assert.deepEqual(directDiff, listDiff);
    let exMessage = '';
    try {
      await l2.diff(l1, 50);
    } catch (ex) {
      exMessage = ex.message;
    }

    assert.strictEqual('Load limit exceeded', exMessage);
  });
});
