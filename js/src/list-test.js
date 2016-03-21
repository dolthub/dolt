// @flow

import {assert} from 'chai';
import {suite} from 'mocha';

import MemoryStore from './memory-store.js';
import test from './async-test.js';
import {calcSplices} from './edit-distance.js';
import {flatten, flattenParallel} from './test-util.js';
import {IndexedMetaSequence, MetaTuple} from './meta-sequence.js';
import {invariant} from './assert.js';
import {Kind} from './noms-kind.js';
import {ListLeafSequence, newList, NomsList} from './list.js';
import {makeCompoundType, makePrimitiveType} from './type.js';
import {DataStore} from './data-store.js';

const testListSize = 5000;
const listOfNRef = 'sha1-11e947e8aacfda8e9052bb57e661da442b26c625';

async function assertToJS(list: NomsList, nums: Array<any>, start: number = 0,
    end: number = nums.length): Promise<void> {
  const jsArray = await list.toJS(start, end);
  const expect = nums.slice(start, end);
  assert.deepEqual(expect, jsArray);
}

suite('BuildList', () => {
  function intSequence(start: number, end: number): Array<number> {
    const nums = [];

    for (let i = start; i < end; i++) {
      nums.push(i);
    }

    return nums;
  }

  function firstNNumbers(n: number): Array<number> {
    return intSequence(0, n);
  }

  test('set of n numbers, length', async () => {
    const nums = firstNNumbers(testListSize);
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int64));
    const s = await newList(nums, tr);
    assert.strictEqual(s.ref.toString(), listOfNRef);
    assert.strictEqual(testListSize, s.length);
  });

  test('toJS', async () => {
    const nums = firstNNumbers(5000);
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int64));
    const s = await newList(nums, tr);
    assert.strictEqual(s.ref.toString(), listOfNRef);
    assert.strictEqual(testListSize, s.length);

    await assertToJS(s, nums, 1000, 2000);
    await assertToJS(s, nums, 3000, 3500);
    await assertToJS(s, nums);
    await assertToJS(s, nums, 0, -100);
    await assertToJS(s, nums, -300, -100);
    await assertToJS(s, nums, -2000, 4000);
    await assertToJS(s, nums, -300, -300);
    await assertToJS(s, nums, -300, -400);
    await assertToJS(s, nums, 10000, 10000);
    await assertToJS(s, nums, 0, 1);
    await assertToJS(s, nums, -1);
  });

  test('insert', async () => {
    const nums = firstNNumbers(testListSize - 10);
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int64));
    let s = await newList(nums, tr);

    for (let i = testListSize - 10; i < testListSize; i++) {
      s = await s.insert(i, [i]);
    }

    assert.strictEqual(s.ref.toString(), listOfNRef);
  });

  test('append', async () => {
    const nums = firstNNumbers(testListSize - 10);
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int64));
    let s = await newList(nums, tr);

    for (let i = testListSize - 10; i < testListSize; i++) {
      s = await s.append([i]);
    }

    assert.strictEqual(s.ref.toString(), listOfNRef);
  });

  test('remove', async () => {
    const nums = firstNNumbers(testListSize + 10);
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int64));
    let s = await newList(nums, tr);

    let count = 10;
    while (count-- > 0) {
      s = await s.remove(testListSize + count, testListSize + count + 1);
    }

    assert.strictEqual(s.ref.toString(), listOfNRef);
  });

  test('splice', async () => {
    const nums = firstNNumbers(testListSize);
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int64));
    let s = await newList(nums, tr);

    const splice500At = async (idx: number) => {
      s = await s.splice(idx, [], 500);
      s = await s.splice(idx, intSequence(idx, idx + 500), 0);
    };


    for (let i = 0; i < testListSize / 1000; i++) {
      await splice500At(i * 1000);
    }

    assert.strictEqual(s.ref.toString(), listOfNRef);
  });

  test('write, read, modify, read', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);

    const nums = firstNNumbers(testListSize);
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int64));
    const s = await newList(nums, tr);
    const r = ds.writeValue(s, tr);
    const s2 = await ds.readValue(r);
    const outNums = await s2.toJS();
    assert.deepEqual(nums, outNums);

    invariant(s2 instanceof NomsList);
    const s3 = await s2.splice(testListSize - 1, [], 1);
    const outNums2 = await s3.toJS();
    nums.splice(testListSize - 1, 1);
    assert.deepEqual(nums, outNums2);
  });
});

suite('ListLeafSequence', () => {
  test('isEmpty', () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.String));
    const newList = items => new NomsList(tr, new ListLeafSequence(ds, tr, items));
    assert.isTrue(newList([]).isEmpty());
    assert.isFalse(newList(['z', 'x', 'a', 'b']).isEmpty());
  });

  test('get', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.String));
    const l = new NomsList(tr, new ListLeafSequence(ds, tr, ['z', 'x', 'a', 'b']));
    assert.strictEqual('z', await l.get(0));
    assert.strictEqual('x', await l.get(1));
    assert.strictEqual('a', await l.get(2));
    assert.strictEqual('b', await l.get(3));
  });

  test('forEach', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int32));
    const l = new NomsList(tr, new ListLeafSequence(ds, tr, [4, 2, 10, 16]));

    const values = [];
    await l.forEach((v, i) => { values.push(v, i); });
    assert.deepEqual([4, 0, 2, 1, 10, 2, 16, 3], values);
  });

  test('iterator', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int32));

    const test = async items => {
      const l = new NomsList(tr, new ListLeafSequence(ds, tr, items));
      assert.deepEqual(items, await flatten(l.iterator()));
      assert.deepEqual(items, await flattenParallel(l.iterator(), items.length));
    };

    await test([]);
    await test([42]);
    await test([4, 2, 10, 16]);
  });

  test('iteratorAt', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int32));

    const test = async items => {
      const l = new NomsList(tr, new ListLeafSequence(ds, tr, items));
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

  test('chunks', () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Value));
    const st = makePrimitiveType(Kind.String);
    const r1 = ds.writeValue('x', st);
    const r2 = ds.writeValue('a', st);
    const r3 = ds.writeValue('b', st);
    const l = new NomsList(tr, new ListLeafSequence(ds, tr, ['z', r1, r2, r3]));
    assert.strictEqual(3, l.chunks.length);
    assert.isTrue(r1.equals(l.chunks[0]));
    assert.isTrue(r2.equals(l.chunks[1]));
    assert.isTrue(r3.equals(l.chunks[2]));
  });
});

suite('CompoundList', () => {
  function build(): NomsList {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.String));
    const l1 = new NomsList(tr, new ListLeafSequence(ds, tr, ['a', 'b']));
    const r1 = ds.writeValue(l1, tr);
    const l2 = new NomsList(tr, new ListLeafSequence(ds, tr, ['e', 'f']));
    const r2 = ds.writeValue(l2, tr);
    const l3 = new NomsList(tr, new ListLeafSequence(ds, tr, ['h', 'i']));
    const r3 = ds.writeValue(l3, tr);
    const l4 = new NomsList(tr, new ListLeafSequence(ds, tr, ['m', 'n']));
    const r4 = ds.writeValue(l4, tr);

    const m1 = new NomsList(tr, new IndexedMetaSequence(ds, tr, [new MetaTuple(r1, 2),
        new MetaTuple(r2, 2)]));
    const rm1 = ds.writeValue(m1, tr);
    const m2 = new NomsList(tr, new IndexedMetaSequence(ds, tr, [new MetaTuple(r3, 2),
        new MetaTuple(r4, 2)]));
    const rm2 = ds.writeValue(m2, tr);

    const l = new NomsList(tr, new IndexedMetaSequence(ds, tr, [new MetaTuple(rm1, 4),
        new MetaTuple(rm2, 4)]));
    return l;
  }

  test('isEmpty', () => {
    assert.isFalse(build().isEmpty());
  });

  test('toJS', async () => {
    const l = build();
    await assertToJS(l, ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n'] , 0, 8);
  });

  test('get', async () => {
    const l = build();
    assert.strictEqual('a', await l.get(0));
    assert.strictEqual('b', await l.get(1));
    assert.strictEqual('e', await l.get(2));
    assert.strictEqual('f', await l.get(3));
    assert.strictEqual('h', await l.get(4));
    assert.strictEqual('i', await l.get(5));
    assert.strictEqual('m', await l.get(6));
    assert.strictEqual('n', await l.get(7));
  });

  test('forEach', async () => {
    const l = build();
    const values = [];
    await l.forEach((k, i) => { values.push(k, i); });
    assert.deepEqual(['a', 0, 'b', 1, 'e', 2, 'f', 3, 'h', 4, 'i', 5, 'm', 6, 'n', 7], values);
  });

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

  test('chunks', () => {
    const l = build();
    assert.strictEqual(2, l.chunks.length);
  });

  test('length', () => {
    const l = build();
    assert.equal(l.length, 8);
  });
});

suite('Diff List', () => {
  function intSequence(start: number, end: number): Array<number> {
    const nums = [];

    for (let i = start; i < end; i++) {
      nums.push(i);
    }

    return nums;
  }

  function firstNNumbers(n: number): Array<number> {
    return intSequence(0, n);
  }

  test('Remove 5x100', async () => {
    const nums1 = firstNNumbers(5000);
    const nums2 = nums1.slice(0);

    let count = 5;
    while (count-- > 0) {
      nums2.splice(count * 1000, 100);
    }

    const directDiff = calcSplices(nums1.length, nums2.length, (i, j) => nums1[i] === nums2[j]);

    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int64));
    const l1 = await newList(nums1, tr);
    const l2 = await newList(nums2, tr);

    const listDiff = await l2.diff(l1);
    assert.deepEqual(directDiff, listDiff);
  });

  test('Add 5x5', async () => {
    const nums1 = firstNNumbers(5000);
    const nums2 = nums1.slice(0);

    let count = 5;
    while (count-- > 0) {
      nums2.splice(count * 1000, 0, 0, 1, 2, 3, 4);
    }

    const directDiff = calcSplices(nums1.length, nums2.length, (i, j) => nums1[i] === nums2[j]);

    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int64));
    const l1 = await newList(nums1, tr);
    const l2 = await newList(nums2, tr);

    const listDiff = await l2.diff(l1);
    assert.deepEqual(directDiff, listDiff);
  });

  test('Replace reverse 5x100', async () => {
    const nums1 = firstNNumbers(5000);
    const nums2 = nums1.slice(0);

    let count = 5;
    while (count-- > 0) {
      const out = nums2.slice(count * 1000, 100).reverse();
      nums2.splice(count * 1000, 100, ...out);
    }

    const directDiff = calcSplices(nums1.length, nums2.length, (i, j) => nums1[i] === nums2[j]);
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int64));
    const l1 = await newList(nums1, tr);
    const l2 = await newList(nums2, tr);

    const listDiff = await l2.diff(l1);
    assert.deepEqual(directDiff, listDiff);
  });

  test('Load Limit', async () => {
    const nums1 = firstNNumbers(5);
    const nums2 = firstNNumbers(5000);

    const directDiff = calcSplices(nums1.length, nums2.length, (i, j) => nums1[i] === nums2[j]);
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int64));
    const l1 = await newList(nums1, tr);
    const l2 = await newList(nums2, tr);

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
