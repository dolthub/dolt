// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';

import DataStore from './data-store.js';
import MemoryStore from './memory-store.js';
import RefValue from './ref-value.js';
import {newStruct} from './struct.js';
import {
  boolType,
  Field,
  int64Type,
  int8Type,
  makeCompoundType,
  makeSetType,
  makeStructType,
  makeType,
  stringType,
  valueType,
} from './type.js';
import {flatten, flattenParallel} from './test-util.js';
import {invariant, notNull} from './assert.js';
import {Kind} from './noms-kind.js';
import {MetaTuple, OrderedMetaSequence} from './meta-sequence.js';
import {newSet, NomsSet, SetLeafSequence} from './set.js';
import {OrderedSequence} from './ordered-sequence.js';
import {Package, registerPackage} from './package.js';
import type {Type} from './type.js';

const testSetSize = 5000;
const setOfNRef = 'sha1-54ff8f84b5f39fe2171572922d067257a57c539c';

suite('BuildSet', () => {
  function firstNNumbers(n: number): Array<number> {
    const nums = [];

    for (let i = 0; i < n; i++) {
      nums.push(i);
    }

    return nums;
  }

  test('set of n numbers', async () => {
    const nums = firstNNumbers(testSetSize);
    const tr = makeCompoundType(Kind.Set, int64Type);
    const s = await newSet(nums, tr);
    assert.strictEqual(s.ref.toString(), setOfNRef);

    // shuffle kvs, and test that the constructor sorts properly
    nums.sort(() => Math.random() > .5 ? 1 : -1);
    const s2 = await newSet(nums, tr);
    assert.strictEqual(s2.ref.toString(), setOfNRef);
  });

  test('set of ref, set of n numbers', async () => {
    const nums = firstNNumbers(testSetSize);

    const structTypeDef = makeStructType('num', [
      new Field('n', int64Type, false),
    ], []);
    const pkg = new Package([structTypeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const structType = makeType(pkgRef, 0);
    const refOfStructType = makeCompoundType(Kind.Ref, structType);
    const tr = makeCompoundType(Kind.Set, refOfStructType);

    const refs = nums.map(n => {
      const s = newStruct(structType, structTypeDef, {n});
      const r = s.ref;
      return new RefValue(r, refOfStructType);
    });

    const s = await newSet(refs, tr);
    assert.strictEqual(s.ref.toString(), 'sha1-3ed56cc080690be61c72828e80080ec3507fec65');
  });


  test('insert', async () => {
    const nums = firstNNumbers(testSetSize - 10);
    const tr = makeCompoundType(Kind.Set, int64Type);
    let s = await newSet(nums, tr);

    for (let i = testSetSize - 10; i < testSetSize; i++) {
      s = await s.insert(i);
    }

    assert.strictEqual(s.ref.toString(), setOfNRef);
  });

  test('remove', async () => {
    const nums = firstNNumbers(testSetSize + 10);
    const tr = makeCompoundType(Kind.Set, int64Type);
    let s = await newSet(nums, tr);

    let count = 10;
    while (count-- > 0) {
      s = await s.remove(testSetSize + count);
    }

    assert.strictEqual(s.ref.toString(), setOfNRef);
  });

  test('write, read, modify, read', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);

    const nums = firstNNumbers(testSetSize);
    const tr = makeCompoundType(Kind.Set, int64Type);
    const s = await newSet(nums, tr);
    const r = ds.writeValue(s);
    const s2 = await ds.readValue(r);
    const outNums = [];
    await s2.forEach(k => outNums.push(k));
    assert.deepEqual(nums, outNums);

    invariant(s2 instanceof NomsSet);
    const s3 = await s2.remove(testSetSize - 1);
    const outNums2 = [];
    await s3.forEach(k => outNums2.push(k));
    nums.splice(testSetSize - 1, 1);
    assert.deepEqual(nums, outNums2);
  });
});

suite('SetLeaf', () => {
  test('isEmpty', () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const tr = makeCompoundType(Kind.Set, stringType);
    const newSet = items => new NomsSet(tr, new SetLeafSequence(ds, tr, items));
    assert.isTrue(newSet([]).isEmpty());
    assert.isFalse(newSet(['a', 'k']).isEmpty());
  });

  test('first/last/has', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const tr = makeCompoundType(Kind.Set, stringType);
    const s = new NomsSet(tr, new SetLeafSequence(ds, tr, ['a', 'k']));

    assert.strictEqual('a', await s.first());
    assert.strictEqual('k', await s.last());

    assert.isTrue(await s.has('a'));
    assert.isFalse(await s.has('b'));
    assert.isTrue(await s.has('k'));
    assert.isFalse(await s.has('z'));
  });

  test('forEach', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const tr = makeCompoundType(Kind.Set, stringType);
    const m = new NomsSet(tr, new SetLeafSequence(ds, tr, ['a', 'b']));

    const values = [];
    await m.forEach((k) => { values.push(k); });
    assert.deepEqual(['a', 'b'], values);
  });

  test('iterator', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const tr = makeCompoundType(Kind.Set, stringType);

    const test = async items => {
      const m = new NomsSet(tr, new SetLeafSequence(ds, tr, items));
      assert.deepEqual(items, await flatten(m.iterator()));
      assert.deepEqual(items, await flattenParallel(m.iterator(), items.length));
    };

    await test([]);
    await test(['a']);
    await test(['a', 'b']);
  });

  test('iteratorAt', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const tr = makeCompoundType(Kind.Set, stringType);
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
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const tr = makeCompoundType(Kind.Set, elemType);
    const st = stringType;
    const refOfSt = makeCompoundType(Kind.Ref, st);
    const r1 = new RefValue(ds.writeValue('x'), refOfSt);
    const r2 = new RefValue(ds.writeValue('a'), refOfSt);
    const r3 = new RefValue(ds.writeValue('b'), refOfSt);
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
  function build(ds: DataStore, values: Array<string>): NomsSet {
    const tr = makeCompoundType(Kind.Set, stringType);
    assert.isTrue(values.length > 1 && Math.log2(values.length) % 1 === 0);

    let tuples = [];
    for (let i = 0; i < values.length; i += 2) {
      const l = new NomsSet(tr, new SetLeafSequence(ds, tr, [values[i], values[i + 1]]));
      const r = ds.writeValue(l);
      tuples.push(new MetaTuple(r, values[i + 1]));
    }

    let last: ?NomsSet = null;
    while (tuples.length > 1) {
      const next = [];
      for (let i = 0; i < tuples.length; i += 2) {
        last = new NomsSet(tr, new OrderedMetaSequence(ds, tr, [tuples[i], tuples[i + 1]]));
        const r = ds.writeValue(last);
        next.push(new MetaTuple(r, tuples[i + 1].value));
      }

      tuples = next;
    }

    return notNull(last);
  }

  test('isEmpty', () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const c = build(ds, ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n']);
    assert.isFalse(c.isEmpty());
  });

  test('first/last/has', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
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
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const c = build(ds, ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n']);
    const values = [];
    await c.forEach((k) => { values.push(k); });
    assert.deepEqual(['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n'], values);
  });

  test('iterator', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const values = ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n'];
    const c = build(ds, values);
    assert.deepEqual(values, await flatten(c.iterator()));
    assert.deepEqual(values, await flattenParallel(c.iterator(), values.length));
  });

  test('iteratorAt', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
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
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
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
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const c = build(ds, ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n']);
    const iter = c.iterator();
    const values = await Promise.all([iter.next(), iter.next(), iter.return(), iter.next()]);
    assert.deepEqual(
        [{done: false, value: 'a'}, {done: false, value: 'b'}, {done: true}, {done: true}],
        values);
  });

  test('chunks', () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const c = build(ds, ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n']);
    assert.strictEqual(2, c.chunks.length);
  });

  test('map', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const c = build(ds, ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n']);
    const values = await c.map((k) => k + '*');
    assert.deepEqual(['a*', 'b*', 'e*', 'f*', 'h*', 'i*', 'm*', 'n*'], values);
  });

  test('map async', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
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
    const ms = new MemoryStore();
    const ds = new DataStore(ms);

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
    const ms = new MemoryStore();
    const ds = new DataStore(ms);

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

  test('intersect', async () => {
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
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const tr = makeCompoundType(Kind.Set, int8Type);

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
});
