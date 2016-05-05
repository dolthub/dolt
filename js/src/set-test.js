// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';

import Chunk from './chunk.js';
import DataStore from './data-store.js';
import MemoryStore from './memory-store.js';
import RefValue from './ref-value.js';
import BatchStore from './batch-store.js';
import {BatchStoreAdaptorDelegate, makeTestingBatchStore} from './batch-store-adaptor.js';
import {newStruct} from './struct.js';
import {
  boolType,
  makeRefType,
  makeSetType,
  makeStructType,
  numberType,
  stringType,
  valueType,
} from './type.js';
import {flatten, flattenParallel, deriveSequenceHeight} from './test-util.js';
import {invariant, notNull} from './assert.js';
import {MetaTuple, OrderedMetaSequence} from './meta-sequence.js';
import {newSet, NomsSet, SetLeafSequence} from './set.js';
import {OrderedSequence} from './ordered-sequence.js';
import Ref from './ref.js';
import type {Type} from './type.js';
import type {ValueReadWriter} from './value-store.js';

const testSetSize = 5000;
const setOfNRef = 'sha1-5937b476bb1d594e3a905c44f00863bd9ba1fb19';
const smallRandomSetSize = 200;
const randomSetSize = 2000;

class CountingMemoryStore extends MemoryStore {
  getCount: number;

  constructor() {
    super();
    this.getCount = 0;
  }

  get(ref: Ref): Promise<Chunk> {
    this.getCount++;
    return super.get(ref);
  }
}

function firstNNumbers(n: number): Array<number> {
  const nums = [];
  for (let i = 0; i < n; i++) {
    nums.push(i);
  }
  return nums;
}

suite('BuildSet', () => {
  test('unique keys - strings', async () => {
    const strs = ['hello', 'world', 'hello'];
    const tr = makeSetType(stringType);
    const s = await newSet(strs, tr);
    assert.strictEqual(2, s.size);
    assert.isTrue(await s.has('hello'));
    assert.isTrue(await s.has('world'));
    assert.isFalse(await s.has('foo'));
  });

  test('unique keys - number', async () => {
    const nums = [4, 1, 0, 0, 1, 3];
    const tr = makeSetType(numberType);
    const s = await newSet(nums, tr);
    assert.strictEqual(4, s.size);
    assert.isTrue(await s.has(4));
    assert.isTrue(await s.has(1));
    assert.isTrue(await s.has(0));
    assert.isTrue(await s.has(3));
    assert.isFalse(await s.has(2));
  });

  test('LONG: set of n numbers', async () => {
    const nums = firstNNumbers(testSetSize);
    const tr = makeSetType(numberType);
    const s = await newSet(nums, tr);
    assert.strictEqual(s.ref.toString(), setOfNRef);

    // shuffle kvs, and test that the constructor sorts properly
    nums.sort(() => Math.random() > .5 ? 1 : -1);
    const s2 = await newSet(nums, tr);
    assert.strictEqual(s2.ref.toString(), setOfNRef);
  });

  test('LONG: set of ref, set of n numbers', async () => {
    const nums = firstNNumbers(testSetSize);

    const structType = makeStructType('num', {
      'n': numberType,
    });
    const refOfStructType = makeRefType(structType);
    const tr = makeSetType(refOfStructType);

    const refs = nums.map(n => new RefValue(newStruct(structType, {n})));
    const s = await newSet(refs, tr);
    assert.strictEqual(s.ref.toString(), 'sha1-3664c45fcbf64f1272956a7b93f27488ab0eb4f8');
    const height = deriveSequenceHeight(s.sequence);
    assert.isTrue(height > 0);
    // height + 1 because the leaves are RefValue values (with height 1).
    assert.strictEqual(height + 1, s.sequence.items[0].ref.height);
  });

  test('LONG: insert', async () => {
    const nums = firstNNumbers(testSetSize - 10);
    const tr = makeSetType(numberType);
    let s = await newSet(nums, tr);
    for (let i = testSetSize - 10; i < testSetSize; i++) {
      s = await s.insert(i);
      assert.strictEqual(i + 1, s.size);
    }

    assert.strictEqual(s.ref.toString(), 'sha1-c99cd8be7a36678e4a538cf23496dfc96beaa64f');
  });

  test('LONG: remove', async () => {
    const nums = firstNNumbers(testSetSize + 10);
    const tr = makeSetType(numberType);
    let s = await newSet(nums, tr);
    let count = 10;
    while (count-- > 0) {
      s = await s.remove(testSetSize + count);
      assert.strictEqual(testSetSize + count, s.size);
    }

    assert.strictEqual(s.ref.toString(), setOfNRef);
  });

  test('LONG: write, read, modify, read', async () => {
    const ds = new DataStore(makeTestingBatchStore());

    const nums = firstNNumbers(testSetSize);
    const tr = makeSetType(numberType);
    const s = await newSet(nums, tr);
    const r = ds.writeValue(s).targetRef;
    const s2 = await ds.readValue(r);
    const outNums = [];
    await s2.forEach(k => outNums.push(k));
    assert.deepEqual(nums, outNums);
    assert.strictEqual(testSetSize, s2.size);

    invariant(s2 instanceof NomsSet);
    const s3 = await s2.remove(testSetSize - 1);
    const outNums2 = [];
    await s3.forEach(k => outNums2.push(k));
    nums.splice(testSetSize - 1, 1);
    assert.deepEqual(nums, outNums2);
    assert.strictEqual(testSetSize - 1, s3.size);
  });
});

suite('SetLeaf', () => {
  test('isEmpty/size', () => {
    const ds = new DataStore(makeTestingBatchStore());
    const tr = makeSetType(stringType);
    const newSet = items => new NomsSet(tr, new SetLeafSequence(ds, tr, items));
    let s = newSet([]);
    assert.isTrue(s.isEmpty());
    assert.strictEqual(0, s.size);
    s = newSet(['a', 'k']);
    assert.isFalse(s.isEmpty());
    assert.strictEqual(2, s.size);
  });

  test('first/last/has', async () => {
    const ds = new DataStore(makeTestingBatchStore());
    const tr = makeSetType(stringType);
    const s = new NomsSet(tr, new SetLeafSequence(ds, tr, ['a', 'k']));

    assert.strictEqual('a', await s.first());
    assert.strictEqual('k', await s.last());

    assert.isTrue(await s.has('a'));
    assert.isFalse(await s.has('b'));
    assert.isTrue(await s.has('k'));
    assert.isFalse(await s.has('z'));
  });

  test('forEach', async () => {
    const ds = new DataStore(makeTestingBatchStore());
    const tr = makeSetType(stringType);
    const m = new NomsSet(tr, new SetLeafSequence(ds, tr, ['a', 'b']));

    const values = [];
    await m.forEach((k) => { values.push(k); });
    assert.deepEqual(['a', 'b'], values);
  });

  test('iterator', async () => {
    const ds = new DataStore(makeTestingBatchStore());
    const tr = makeSetType(stringType);

    const test = async items => {
      const m = new NomsSet(tr, new SetLeafSequence(ds, tr, items));
      assert.deepEqual(items, await flatten(m.iterator()));
      assert.deepEqual(items, await flattenParallel(m.iterator(), items.length));
    };

    await test([]);
    await test(['a']);
    await test(['a', 'b']);
  });

  test('LONG: iteratorAt', async () => {
    const ds = new DataStore(makeTestingBatchStore());
    const tr = makeSetType(stringType);
    const build = items => new NomsSet(tr, new SetLeafSequence(ds, tr, items));

    assert.deepEqual([], await flatten(build([]).iteratorAt('a')));

    assert.deepEqual(['b'], await flatten(build(['b']).iteratorAt('a')));
    assert.deepEqual(['b'], await flatten(build(['b']).iteratorAt('b')));
    assert.deepEqual([], await flatten(build(['b']).iteratorAt('c')));

    assert.deepEqual(['b', 'd'], await flatten(build(['b', 'd']).iteratorAt('a')));
    assert.deepEqual(['b', 'd'], await flatten(build(['b', 'd']).iteratorAt('b')));
    assert.deepEqual(['d'], await flatten(build(['b', 'd']).iteratorAt('c')));
    assert.deepEqual(['d'], await flatten(build(['b', 'd']).iteratorAt('d')));
    assert.deepEqual([], await flatten(build(['b', 'd']).iteratorAt('e')));
  });

  function testChunks(elemType: Type) {
    const ds = new DataStore(makeTestingBatchStore());
    const tr = makeSetType(elemType);
    const r1 = ds.writeValue('x');
    const r2 = ds.writeValue('a');
    const r3 = ds.writeValue('b');
    const l = new NomsSet(tr, new SetLeafSequence(ds, tr, ['z', r1, r2, r3]));
    assert.strictEqual(3, l.chunks.length);
    assert.isTrue(r1.equals(l.chunks[0]));
    assert.isTrue(r2.equals(l.chunks[1]));
    assert.isTrue(r3.equals(l.chunks[2]));
  }

  test('chunks, set of value', () => {
    testChunks(valueType);
  });

  test('chunks', () => {
    testChunks(stringType);
  });
});

suite('CompoundSet', () => {
  function build(vwr: ValueReadWriter, values: Array<string>): NomsSet {
    const tr = makeSetType(stringType);
    assert.isTrue(values.length > 1 && Math.log2(values.length) % 1 === 0);

    let tuples = [];
    for (let i = 0; i < values.length; i += 2) {
      const l = new NomsSet(tr, new SetLeafSequence(vwr, tr, [values[i], values[i + 1]]));
      const r = vwr.writeValue(l);
      tuples.push(new MetaTuple(r, values[i + 1], 2));
    }

    let last: ?NomsSet = null;
    while (tuples.length > 1) {
      const next = [];
      for (let i = 0; i < tuples.length; i += 2) {
        last = new NomsSet(tr, new OrderedMetaSequence(vwr, tr, [tuples[i], tuples[i + 1]]));
        const r = vwr.writeValue(last);
        next.push(new MetaTuple(r, tuples[i + 1].value,
                                tuples[i].numLeaves + tuples[i + 1].numLeaves));
      }

      tuples = next;
    }

    return notNull(last);
  }

  test('isEmpty/size', () => {
    const ds = new DataStore(makeTestingBatchStore());
    const c = build(ds, ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n']);
    assert.isFalse(c.isEmpty());
    assert.strictEqual(8, c.size);
  });

  test('first/last/has', async () => {
    const ds = new DataStore(makeTestingBatchStore());
    const c = build(ds, ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n']);
    assert.strictEqual('a', await c.first());
    assert.strictEqual('n', await c.last());
    assert.isTrue(await c.has('a'));
    assert.isTrue(await c.has('b'));
    assert.isFalse(await c.has('c'));
    assert.isFalse(await c.has('d'));
    assert.isTrue(await c.has('e'));
    assert.isTrue(await c.has('f'));
    assert.isTrue(await c.has('h'));
    assert.isTrue(await c.has('i'));
    assert.isFalse(await c.has('j'));
    assert.isFalse(await c.has('k'));
    assert.isFalse(await c.has('l'));
    assert.isTrue(await c.has('m'));
    assert.isTrue(await c.has('n'));
    assert.isFalse(await c.has('o'));
  });

  test('forEach', async () => {
    const ds = new DataStore(makeTestingBatchStore());
    const c = build(ds, ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n']);
    const values = [];
    await c.forEach((k) => { values.push(k); });
    assert.deepEqual(['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n'], values);
  });

  test('iterator', async () => {
    const ds = new DataStore(makeTestingBatchStore());
    const values = ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n'];
    const c = build(ds, values);
    assert.deepEqual(values, await flatten(c.iterator()));
    assert.deepEqual(values, await flattenParallel(c.iterator(), values.length));
  });

  test('LONG: iteratorAt', async () => {
    const ds = new DataStore(makeTestingBatchStore());
    const values = ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n'];
    const c = build(ds, values);
    const offsets = {
      _: 0, a: 0,
      b: 1,
      c: 2, d: 2, e: 2,
      f: 3,
      g: 4, h: 4,
      i: 5,
      j: 6, k: 6, l: 6, m: 6,
      n: 7,
      o: 8,
    };
    for (const k in offsets) {
      const slice = values.slice(offsets[k]);
      assert.deepEqual(slice, await flatten(c.iteratorAt(k)));
      assert.deepEqual(slice, await flattenParallel(c.iteratorAt(k), slice.length));
    }
  });

  test('iterator return', async () => {
    const ds = new DataStore(makeTestingBatchStore());
    const values = ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n'];
    const c = build(ds, values);
    const iter = c.iterator();
    const values2 = [];
    for (let res = await iter.next(); !res.done; res = await iter.next()) {
      values2.push(res.value);
      if (values2.length === 5) {
        await iter.return();
      }
    }
    assert.deepEqual(values.slice(0, 5), values2);
  });

  test('iterator return parallel', async () => {
    const ds = new DataStore(makeTestingBatchStore());
    const c = build(ds, ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n']);
    const iter = c.iterator();
    const values = await Promise.all([iter.next(), iter.next(), iter.return(), iter.next()]);
    assert.deepEqual(
        [{done: false, value: 'a'}, {done: false, value: 'b'}, {done: true}, {done: true}],
        values);
  });

  test('chunks', () => {
    const ds = new DataStore(makeTestingBatchStore());
    const c = build(ds, ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n']);
    assert.strictEqual(2, c.chunks.length);
  });

  test('map', async () => {
    const ds = new DataStore(makeTestingBatchStore());
    const c = build(ds, ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n']);
    const values = await c.map((k) => k + '*');
    assert.deepEqual(['a*', 'b*', 'e*', 'f*', 'h*', 'i*', 'm*', 'n*'], values);
  });

  test('map async', async () => {
    const ds = new DataStore(makeTestingBatchStore());
    const c = build(ds, ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n']);
    const values = await c.map((k) => Promise.resolve(k + '*'));
    assert.deepEqual(['a*', 'b*', 'e*', 'f*', 'h*', 'i*', 'm*', 'n*'], values);
  });

  async function asyncAssertThrows(f: () => any):Promise<boolean> {
    let error: any = null;
    try {
      await f();
    } catch (er) {
      error = er;
    }

    return error !== null;
  }

  test('advanceTo', async () => {
    const ds = new DataStore(makeTestingBatchStore());

    const c = build(ds, ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n']);

    invariant(c.sequence instanceof OrderedSequence);
    let cursor = await c.sequence.newCursorAt(null);
    assert.ok(cursor);
    assert.strictEqual('a', cursor.getCurrent());

    assert.isTrue(await cursor.advanceTo('h'));
    assert.strictEqual('h', cursor.getCurrent());

    assert.isTrue(await cursor.advanceTo('k'));
    assert.strictEqual('m', cursor.getCurrent());

    assert.isFalse(await cursor.advanceTo('z')); // not found
    assert.isFalse(cursor.valid);

    invariant(c.sequence instanceof OrderedSequence);
    cursor = await c.sequence.newCursorAt('x'); // not found
    assert.isFalse(cursor.valid);

    invariant(c.sequence instanceof OrderedSequence);
    cursor = await c.sequence.newCursorAt('e');
    assert.ok(cursor);
    assert.strictEqual('e', cursor.getCurrent());

    assert.isTrue(await cursor.advanceTo('m'));
    assert.strictEqual('m', cursor.getCurrent());

    assert.isTrue(await cursor.advanceTo('n'));
    assert.strictEqual('n', cursor.getCurrent());

    assert.isFalse(await cursor.advanceTo('s'));
    assert.isFalse(cursor.valid);

    asyncAssertThrows(async () => {
      await notNull(cursor).advanceTo('x');
    });
  });

  async function testIntersect(expect: Array<string>, seqs: Array<Array<string>>) {
    const ds = new DataStore(makeTestingBatchStore());

    const first = build(ds, seqs[0]);
    const sets:Array<NomsSet> = [];
    for (let i = 1; i < seqs.length; i++) {
      sets.push(build(ds, seqs[i]));
    }

    const result = await first.intersect(...sets);
    const actual = [];
    await result.forEach(v => { actual.push(v); });
    assert.deepEqual(expect, actual);
  }

  test('LONG: intersect', async () => {
    await testIntersect(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'],
        [['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'], ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'],
        ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']]);
    await testIntersect(['a', 'h'], [['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'],
        ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'], ['a', 'h', 'i', 'j', 'k', 'l', 'm', 'n']]);
    await testIntersect(['d', 'e', 'f', 'g', 'h'], [['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'],
        ['d', 'e', 'f', 'g', 'h', 'i', 'j', 'k']]);
    await testIntersect(['h'], [['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'],
        ['d', 'e', 'f', 'g', 'h', 'i', 'j', 'k'], ['h', 'i', 'j', 'k', 'l', 'm', 'n', 'o']]);
    await testIntersect([], [['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'],
        ['d', 'e', 'f', 'g', 'h', 'i', 'j', 'k'], ['i', 'j', 'k', 'l', 'm', 'n', 'o', 'p']]);
  });

  test('iterator at 0', async () => {
    const ds = new DataStore(makeTestingBatchStore());
    const tr = makeSetType(numberType);

    const test = async (expected, items) => {
      const set = new NomsSet(tr, new SetLeafSequence(ds, tr, items));
      const iter = set.iteratorAt(0);
      assert.deepEqual(expected, await flatten(iter));
    };

    await test([1, 2], [1, 2]);
    await test([0, 1, 2], [0, 1, 2]);
    await test([1, 2], [-2, -1, 1, 2]);
    await test([0, 1, 2], [-2, -1, 0, 1, 2]);
  });

  test('set of bool', async () => {
    const tr = makeSetType(boolType);
    const set = await newSet([true], tr);
    assert.isTrue(await set.has(true));
    assert.isFalse(await set.has(false));
  });

  test('LONG: canned set diff', async () => {
    const ds = new DataStore(makeTestingBatchStore());
    const tr = makeSetType(numberType);
    const s1 = await newSet(
      firstNNumbers(testSetSize), tr).then(s => ds.readValue(ds.writeValue(s).targetRef));

    {
      // Insert/remove at start.
      const s2 = await s1.insert(-1);
      assert.deepEqual([[-1], []], await s2.diff(s1));
      assert.deepEqual([[], [-1]], await s1.diff(s2));
    }
    {
      // Insert/remove at end.
      const s2 = await s1.insert(testSetSize);
      assert.deepEqual([[testSetSize], []], await s2.diff(s1));
      assert.deepEqual([[], [testSetSize]], await s1.diff(s2));
    }
    {
      // Insert/remove in middle.
      const s2 = await s1.remove(testSetSize / 2);
      assert.deepEqual([[], [testSetSize / 2]], await s2.diff(s1));
      assert.deepEqual([[testSetSize / 2], []], await s1.diff(s2));
    }
  });

  async function testRandomDiff(setSize: number, inS1: number, inS2: number): Promise<void> {
    invariant(inS1 + inS2 <= 1);

    const tr = makeSetType(numberType);
    const nums1 = [], nums2 = [], added = [], removed = [];

    // Randomly populate nums1/nums2 which will be the contents of s1/s2 respectively, and record
    // which numbers were added/removed.
    for (let i = 0; i < setSize; i++) {
      const r = Math.random();
      if (r <= inS1) {
        nums1.push(i);
        removed.push(i);
      } else if (r <= inS1 + inS2) {
        nums2.push(i);
        added.push(i);
      } else {
        nums1.push(i);
        nums2.push(i);
      }
    }

    let [s1, s2] = await Promise.all([newSet(nums1, tr), newSet(nums2, tr)]);

    if (s1.empty || s2.empty || added.length + removed.length === 0) {
      return testRandomDiff(setSize, inS1, inS2);
    }

    const ms = new CountingMemoryStore();
    const ds = new DataStore(new BatchStore(3, new BatchStoreAdaptorDelegate(ms)));
    [s1, s2] = await Promise.all([s1, s2].map(s => ds.readValue(ds.writeValue(s).targetRef)));

    assert.deepEqual([[], []], await s1.diff(s1));
    assert.deepEqual([[], []], await s2.diff(s2));
    assert.deepEqual([removed, added], await s1.diff(s2));
    assert.deepEqual([added, removed], await s2.diff(s1));
  }

  function testSmallRandomDiff(inS1: number, inS2: number): Promise<void> {
    const rounds = randomSetSize / smallRandomSetSize;
    const tests = [];
    for (let i = 0; i < rounds; i++) {
      tests.push(testRandomDiff(smallRandomSetSize, inS1, inS2));
    }
    return Promise.all(tests).then(() => undefined);
  }

  test('LONG: random small set diff 0.1/0.1', () => testSmallRandomDiff(0.1, 0.1));
  test('LONG: random small set diff 0.1/0.5', () => testSmallRandomDiff(0.1, 0.5));
  test('LONG: random small set diff 0.1/0.9', () => testSmallRandomDiff(0.1, 0.9));

  test('LONG: random set diff 0.0001/0.0001', () => testRandomDiff(randomSetSize, 0.0001, 0.0001));
  test('LONG: random set diff 0.0001/0.5', () => testRandomDiff(randomSetSize, 0.0001, 0.5));
  test('LONG: random set diff 0.0001/0.9999', () => testRandomDiff(randomSetSize, 0.0001, 0.9900));

  test('LONG: random set diff 0.001/0.001', () => testRandomDiff(randomSetSize, 0.001, 0.001));
  test('LONG: random set diff 0.001/0.5', () => testRandomDiff(randomSetSize, 0.001, 0.5));
  test('LONG: random set diff 0.001/0.999', () => testRandomDiff(randomSetSize, 0.001, 0.999));

  test('LONG: random set diff 0.01/0.01', () => testRandomDiff(randomSetSize, 0.01, 0.01));
  test('LONG: random set diff 0.01/0.5', () => testRandomDiff(randomSetSize, 0.01, 0.5));
  test('LONG: random set diff 0.01/0.99', () => testRandomDiff(randomSetSize, 0.01, 0.99));

  test('LONG: random set diff 0.1/0.1', () => testRandomDiff(randomSetSize, 0.1, 0.1));
  test('LONG: random set diff 0.1/0.5', () => testRandomDiff(randomSetSize, 0.1, 0.5));
  test('LONG: random set diff 0.1/0.9', () => testRandomDiff(randomSetSize, 0.1, 0.9));

  test('chunks', () => {
    const ds = new DataStore(makeTestingBatchStore());
    const s = build(ds, ['a', 'b', 'c', 'd']);
    const chunks = s.chunks;
    const sequence = s.sequence;
    assert.equal(2, chunks.length);
    assert.deepEqual(sequence.items[0].ref, chunks[0]);
    assert.deepEqual(sequence.items[1].ref, chunks[1]);
  });
});
