// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {assert} from 'chai';
import {suite, setup, teardown, test} from 'mocha';

import Blob from './blob.js';
import Ref from './ref.js';
import Set from './set.js';
import Struct, {newStruct} from './struct.js';
import {
  assertChunkCountAndType,
  assertValueHash,
  assertValueType,
  deriveCollectionHeight,
  intSequence,
  flatten,
  flattenParallel,
  TestDatabase,
  testRoundTripAndValidate,
} from './test-util.js';
import {getTypeOfValue} from './type.js';
import type Value from './value.js';
import {invariant, notNull} from './assert.js';
import List from './list.js';
import Map, {MapLeafSequence} from './map.js';
import {OrderedKey, MetaTuple, newMapMetaSequence} from './meta-sequence.js';
import type {ValueReadWriter} from './value-store.js';
import {compare, equals} from './compare.js';
import {OrderedMetaSequence} from './meta-sequence.js';
import {smallTestChunks, normalProductionChunks} from './rolling-value-hasher.js';
import {
  makeMapType,
  makeRefType,
  makeUnionType,
  numberType,
  stringType,
} from './type.js';

const testMapSize = 1000;
const mapOfNRef = 'jmtmv5mjipjrt5s6s6d80louisqhnj62';
const smallRandomMapSize = 50;
const randomMapSize = 500;

function intKVs(count: number): [number, number][] {
  const kvs = [];
  for (let i = 0; i < count; i++) {
    kvs.push([i, i + 1]);
  }
  return kvs;
}

async function validateMap(m: Map<any, any>, kvs: [[number, number]]): Promise<void> {
  assert.isTrue(equals(new Map(kvs), m));

  const out = [];
  await m.forEach((v, k) => void(out.push([k, v])));
  assert.deepEqual(kvs, out);
}

suite('BuildMap', () => {
  async function mapTestSuite(size: number, expectHashStr: string, expectChunkCount: number,
                              gen: (i: number) => Value): Promise<void> {
    const length = 1 << size;
    const keys = intSequence(length).map(gen);
    keys.sort(compare);
    const entries = keys.map((k, i) => [k, i * 2]);
    const tr = makeMapType(getTypeOfValue(gen(0)), numberType);
    const map = new Map(entries);

    assertValueType(tr, map);
    assert.strictEqual(length, map.size);
    assertValueHash(expectHashStr, map);
    assertChunkCountAndType(expectChunkCount, makeRefType(tr), map);

    await testRoundTripAndValidate(map, async v2 => {
      const out = await flatten(v2.iterator());
      for (let i = 0; i < entries.length; i++) {
        assert.isTrue(equals(entries[i][0], out[i][0]));
        assert.isTrue(equals(entries[i][1], out[i][1]));
      }
    });
  }

  test('Map 1K', async () => {
    await mapTestSuite(10, 'chqe8pkmi2lhn2buvqai357pgp3sg3t6', 3, i => i);
  });

  test('LONG: Map 4K', async () => {
    await mapTestSuite(12, 'v6qlscpd5j6ba89v5ebkijgci2djcpls', 7, i => i);
  });

  const newNumberStruct = i => newStruct('', {n: i});

  test('Map 1K structs', async () => {
    await mapTestSuite(10, '20b1927mnjqa0aqsn4lf5rv80rdnebru', 3, newNumberStruct);
  });

  test('LONG: Map 4K structs', async () => {
    await mapTestSuite(12, 'q2kgo4jonhgfeoblvlovh2eo7kqjel54', 7, newNumberStruct);
  });

  test('unique keys - strings', async () => {
    const kvs = [
      ['hello', 'world'],
      ['foo', 'bar'],
      ['bar', 'foo'],
      ['hello', 'foo'],
    ];
    const m = new Map(kvs);
    assert.strictEqual(3, m.size);
    assert.strictEqual('foo', await m.get('hello'));
  });

  test('unique keys - number', async () => {
    const kvs = [
      [4, 1],
      [0, 2],
      [1, 2],
      [3, 4],
      [1, 5],
    ];
    const m = new Map(kvs);
    assert.strictEqual(4, m.size);
    assert.strictEqual(5, await m.get(1));
  });

  test('LONG: set of n numbers', () => {
    const kvs = intKVs(testMapSize);
    const m = new Map(kvs);
    assert.strictEqual(m.hash.toString(), mapOfNRef);

    // shuffle kvs, and test that the constructor sorts properly
    const pairs = [];
    for (let i = 0; i < kvs.length; i += 2) {
      pairs.push({k: kvs[i], v: kvs[i + 1]});
    }
    pairs.sort(() => Math.random() > .5 ? 1 : -1);
    kvs.length = 0;
    pairs.forEach(kv => kvs.push(kv.k, kv.v));
    const m2 = new Map(kvs);
    assert.strictEqual(m2.hash.toString(), mapOfNRef);

    const height = deriveCollectionHeight(m);
    assert.isTrue(height > 0);
    assert.strictEqual(height, deriveCollectionHeight(m2));
    assert.strictEqual(height, m.sequence.items[0].ref.height);
  });

  test('LONG: map of ref to ref, set of n numbers', () => {
    const kvs = intKVs(testMapSize);

    const kvRefs = kvs.map(entry => entry.map(n => new Ref(newStruct('num', {n}))));
    const m = new Map(kvRefs);
    assert.strictEqual(m.hash.toString(), 'g49bom2pq40n2v927846vpmc3injuf5a');
    const height = deriveCollectionHeight(m);
    assert.isTrue(height > 0);
    // height + 1 because the leaves are Ref values (with height 1).
    assert.strictEqual(height + 1, m.sequence.items[0].ref.height);
  });

  test('LONG: set', async () => {
    const kvs = [];
    for (let i = 0; i < testMapSize - 10; i++) {
      kvs.push([i, i + 1]);
    }

    let m = new Map(kvs);
    for (let i = testMapSize - 10; i < testMapSize; i++) {
      m = await m.set(i, i + 1);
      assert.strictEqual(i + 1, m.size);
    }

    assert.strictEqual(m.hash.toString(), mapOfNRef);
  });

  async function validateSet(kvs: [[number, number]]): Promise<void> {
    let m = new Map();
    for (let i = 0; i < kvs.length; i++) {
      const kv = kvs[i];
      m = await m.set(kv[0], kv[1]);
      await validateMap(m, kvs.slice(0, i + 1));
    }
  }

  test('LONG: validate - set ascending', async () => {
    await validateSet(intKVs(300));
  });

  test('LONG: set existing', async () => {
    const kvs = intKVs(testMapSize);

    let m = new Map(kvs);
    for (let i = 0; i < testMapSize; i++) {
      m = await m.set(i, i + 1);
      assert.strictEqual(testMapSize, m.size);
    }

    assert.strictEqual(m.hash.toString(), mapOfNRef);
  });

  test('LONG: delete', async () => {
    const kvs = [];
    for (let i = 0; i < testMapSize + 10; i++) {
      kvs.push([i, i + 1]);
    }

    let m = new Map(kvs);
    for (let i = testMapSize; i < testMapSize + 10; i++) {
      m = await m.delete(i);
    }

    assert.strictEqual(m.hash.toString(), mapOfNRef);
    assert.strictEqual(testMapSize, m.size);
  });

  test('LONG: write, read, modify, read', async () => {
    const db = new TestDatabase();

    const kvs = intKVs(testMapSize);

    const m = new Map(kvs);

    const r = db.writeValue(m).targetHash;
    const m2 = await db.readValue(r);
    const outKvs = [];
    await m2.forEach((v, k) => outKvs.push([k, v]));
    assert.deepEqual(kvs, outKvs);
    assert.strictEqual(testMapSize, m2.size);

    invariant(m2 instanceof Map);
    const m3 = await m2.delete(testMapSize - 1);
    const outKvs2 = [];
    await m3.forEach((v, k) => outKvs2.push([k, v]));
    kvs.splice(testMapSize * 1 - 1, 1);
    assert.deepEqual(kvs, outKvs2);
    assert.strictEqual(testMapSize - 1, m3.size);
    await db.close();
  });

  test('LONG: union write, read, modify, read', async () => {
    const db = new TestDatabase();

    const keys = [];
    const kvs = [];
    const numbers = [];
    const strings = [];
    const structs = [];
    for (let i = 0; i < testMapSize; i++) {
      let v = i;
      if (i % 3 === 0) {
        v = String(v);
        strings.push(v);
      } else if (v % 3 === 1) {
        v = await newStruct('num', {n: v});
        structs.push(v);
      } else {
        numbers.push(v);
      }
      kvs.push([v, i]);
      keys.push(v);
    }

    strings.sort();
    structs.sort(compare);
    const sortedKeys = numbers.concat(strings, structs);

    const m = new Map(kvs);
    assert.strictEqual('qkamb8o40k3kib3ufjlevgtr92oorppd', m.hash.toString());
    const height = deriveCollectionHeight(m);
    assert.isTrue(height > 0);
    assert.strictEqual(height, m.sequence.items[0].ref.height);

    // has
    for (let i = 0; i < keys.length; i += 5) {
      assert.isTrue(await m.has(keys[i]));
    }

    const r = db.writeValue(m).targetHash;
    const m2 = await db.readValue(r);
    const outVals = [];
    const outKeys = [];
    await m2.forEach((v, k) => {
      outVals.push(v);
      outKeys.push(k);
    });
    assert.equal(testMapSize, m2.size);

    function assertEqualVal(k, v) {
      if (k instanceof Struct) {
        assert.equal(k.n, v);
      } else if (typeof k === 'string') {
        assert.equal(Number(k), v);
      } else {
        assert.equal(k, v);
      }
    }

    for (let i = 0; i < sortedKeys.length; i += 5) {
      const k = sortedKeys[i];
      assert.isTrue(equals(k, outKeys[i]));
      const v = await m2.get(k);
      assertEqualVal(k, v);
    }

    invariant(m2 instanceof Map);
    const m3 = await m2.delete(sortedKeys[testMapSize - 1]);  // removes struct
    const outVals2 = [];
    const outKeys2 = [];
    await m2.forEach((v, k) => {
      outVals2.push(v);
      outKeys2.push(k);
    });
    outVals2.splice(testMapSize - 1, 1);
    outKeys2.splice(testMapSize - 1, 1);
    assert.equal(testMapSize - 1, m3.size);
    for (let i = outKeys2.length - 1; i >= 0; i -= 5) {
      const k = sortedKeys[i];
      assert.isTrue(equals(k, outKeys[i]));
      const v = await m3.get(k);
      assertEqualVal(k, v);
    }
    await db.close();
  });

});

suite('MapLeaf', () => {
  let db;

  setup(() => {
    db = new TestDatabase();
  });

  teardown((): Promise<void> => db.close());

  test('isEmpty/size', () => {
    let m = new Map();
    assert.isTrue(m.isEmpty());
    assert.strictEqual(0, m.size);
    m = new Map([['a', false], ['k', true]]);
    assert.isFalse(m.isEmpty());
    assert.strictEqual(2, m.size);
  });

  test('has', async () => {
    const m = new Map([['a', false], ['k', true]]);
    assert.isTrue(await m.has('a'));
    assert.isFalse(await m.has('b'));
    assert.isTrue(await m.has('k'));
    assert.isFalse(await m.has('z'));
  });

  test('first/last/get', async () => {
    const m = new Map([['a', 4], ['k', 8]]);

    assert.deepEqual(['a', 4], await m.first());
    assert.deepEqual(['k', 8], await m.last());

    assert.strictEqual(4, await m.get('a'));
    assert.strictEqual(undefined, await m.get('b'));
    assert.strictEqual(8, await m.get('k'));
    assert.strictEqual(undefined, await m.get('z'));
  });

  test('forEach', async () => {
    const m = new Map([['a', 4], ['k', 8]]);

    const kv = [];
    await m.forEach((v, k) => { kv.push(k, v); });
    assert.deepEqual(['a', 4, 'k', 8], kv);
  });

  test('forEachAsyncCB', async () => {
    const m = new Map([['a', 4], ['k', 8]]);

    let resolver = null;
    const p = new Promise(resolve => resolver = resolve);

    const kv = [];
    const foreachPromise = m.forEach((v, k) => p.then(() => {
      kv.push(k, v);
    }));

    notNull(resolver)();
    return foreachPromise.then(() => assert.deepEqual(['a', 4, 'k', 8], kv));
  });

  test('iterator', async () => {
    const test = async entries => {
      const m = new Map(entries);
      assert.deepEqual(entries, await flatten(m.iterator()));
      assert.deepEqual(entries, await flattenParallel(m.iterator(), entries.length));
    };

    await test([]);
    await test([['a', 4]]);
    await test([['a', 4], ['k', 8]]);
  });

  test('LONG: iteratorAt', async () => {
    const build = entries => new Map(entries);

    assert.deepEqual([], await flatten(build([]).iteratorAt('a')));

    {
      const kv = [['b', 5]];
      assert.deepEqual(kv, await flatten(build(kv).iteratorAt('a')));
      assert.deepEqual(kv, await flatten(build(kv).iteratorAt('b')));
      assert.deepEqual([], await flatten(build(kv).iteratorAt('c')));
    }

    {
      const kv = [['b', 5], ['d', 10]];
      assert.deepEqual(kv, await flatten(build(kv).iteratorAt('a')));
      assert.deepEqual(kv, await flatten(build(kv).iteratorAt('b')));
      assert.deepEqual(kv.slice(1), await flatten(build(kv).iteratorAt('c')));
      assert.deepEqual(kv.slice(1), await flatten(build(kv).iteratorAt('d')));
      assert.deepEqual([], await flatten(build(kv).iteratorAt('e')));
    }
  });

  test('chunks', () => {
    const r1 = db.writeValue('b');
    const r2 = db.writeValue(false);
    const r3 = db.writeValue('x');
    const r4 = db.writeValue(true);

    const m = new Map([[r1, r2], [r3, r4]]);
    assert.strictEqual(4, m.chunks.length);
    assert.isTrue(equals(r1, m.chunks[0]));
    assert.isTrue(equals(r2, m.chunks[1]));
    assert.isTrue(equals(r3, m.chunks[2]));
    assert.isTrue(equals(r4, m.chunks[3]));
  });
});

suite('CompoundMap', () => {
  let db;

  setup(() => {
    smallTestChunks();
    db = new TestDatabase();
  });

  teardown(async () => {
    normalProductionChunks();
    await db.close();
  });

  function build(vwr: ValueReadWriter): Array<Map<any, any>> {
    const l1 = new Map([['a', false], ['b', false]]);
    const r1 = vwr.writeValue(l1);
    const l2 = new Map([['e', true], ['f', true]]);
    const r2 = vwr.writeValue(l2);
    const l3 = new Map([['h', false], ['i', true]]);
    const r3 = vwr.writeValue(l3);
    const l4 = new Map([['m', true], ['n', false]]);
    const r4 = vwr.writeValue(l4);

    const m1 = Map.fromSequence(newMapMetaSequence(vwr, [
      new MetaTuple(r1, new OrderedKey('b'), 2, null),
      new MetaTuple(r2, new OrderedKey('f'), 2, null),
    ]));
    const rm1 = vwr.writeValue(m1);
    const m2 = Map.fromSequence(newMapMetaSequence(vwr, [
      new MetaTuple(r3, new OrderedKey('i'), 2, null),
      new MetaTuple(r4, new OrderedKey('n'), 2, null),
    ]));
    const rm2 = vwr.writeValue(m2);

    const c = Map.fromSequence(newMapMetaSequence(vwr, [
      new MetaTuple(rm1, new OrderedKey('f'), 4, null),
      new MetaTuple(rm2, new OrderedKey('n'), 4, null),
    ]));
    return [c, m1, m2];
  }

  test('isEmpty/size', () => {
    const [c] = build(db);
    assert.isFalse(c.isEmpty());
    assert.strictEqual(8, c.size);
  });

  test('get', async () => {
    const [c] = build(db);

    assert.strictEqual(false, await c.get('a'));
    assert.strictEqual(false, await c.get('b'));
    assert.strictEqual(undefined, await c.get('c'));
    assert.strictEqual(undefined, await c.get('d'));
    assert.strictEqual(true, await c.get('e'));
    assert.strictEqual(true, await c.get('f'));
    assert.strictEqual(false, await c.get('h'));
    assert.strictEqual(true, await c.get('i'));
    assert.strictEqual(undefined, await c.get('j'));
    assert.strictEqual(undefined, await c.get('k'));
    assert.strictEqual(undefined, await c.get('l'));
    assert.strictEqual(true, await c.get('m'));
    assert.strictEqual(false, await c.get('n'));
    assert.strictEqual(undefined, await c.get('o'));
  });

  test('first/last/has', async () => {
    const [c, m1, m2] = build(db);

    assert.deepEqual(['a', false], await c.first());
    assert.deepEqual(['n', false], await c.last());
    assert.deepEqual(['a', false], await m1.first());
    assert.deepEqual(['f', true], await m1.last());
    assert.deepEqual(['h', false], await m2.first());
    assert.deepEqual(['n', false], await m2.last());

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
    const [c] = build(db);

    const kv = [];
    await c.forEach((v, k) => { kv.push(k, v); });
    assert.deepEqual(['a', false, 'b', false, 'e', true, 'f', true, 'h', false, 'i', true, 'm',
        true, 'n', false], kv);
  });

  test('forEachAsyncCB', async () => {
    const [c] = build(db);

    let resolver = null;
    const p = new Promise(resolve => resolver = resolve);

    const kv = [];
    const foreachPromise = c.forEach((v, k) => p.then(() => {
      kv.push(k, v);
    }));

    notNull(resolver)();
    return foreachPromise.then(() => {
      assert.deepEqual(['a', false, 'b', false, 'e', true, 'f', true, 'h', false, 'i', true, 'm',
        true, 'n', false], kv);
    });
  });

  test('iterator', async () => {
    const [c] = build(db);
    const expected = [['a', false], ['b', false], ['e', true], ['f', true], ['h', false],
                      ['i', true], ['m', true], ['n', false]];
    assert.deepEqual(expected, await flatten(c.iterator()));
    assert.deepEqual(expected, await flattenParallel(c.iterator(), expected.length));
  });

  test('LONG: iteratorAt', async () => {
    const [c] = build(db);
    const entries = [['a', false], ['b', false], ['e', true], ['f', true], ['h', false],
                     ['i', true], ['m', true], ['n', false]];
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
      const slice = entries.slice(offsets[k]);
      assert.deepEqual(slice, await flatten(c.iteratorAt(k)));
      assert.deepEqual(slice, await flattenParallel(c.iteratorAt(k), slice.length));
    }
  });

  test('iterator return', async () => {
    const [c] = build(db);
    const iter = c.iterator();
    const values = [];
    for (let res = await iter.next(); !res.done; res = await iter.next()) {
      values.push(res.value);
      if (values.length === 5) {
        await iter.return();
      }
    }
    assert.deepEqual([['a', false], ['b', false], ['e', true], ['f', true], ['h', false]],
                     values);
  });

  test('iterator return parallel', async () => {
    const [c] = build(db);
    const iter = c.iterator();
    const values = await Promise.all([iter.next(), iter.next(), iter.return(), iter.next()]);
    assert.deepEqual([{done: false, value: ['a', false]},
                      {done: false, value: ['b', false]},
                      {done: true}, {done: true}],
                     values);
  });

  test('chunks', () => {
    const [c] = build(db);
    assert.strictEqual(2, c.chunks.length);
  });

  async function testRandomDiff(mapSize: number, inM1: number, inM2: number, inBoth: number) {
    invariant(inM1 + inM2 + inBoth <= 1);

    const kv1 = [], kv2 = [], added = [], removed = [], modified = [];

    // Randomly populate kv1/kv2 which will be the contents of m1/m2 respectively, and record which
    // numbers were added/removed.
    for (let i = 0; i < mapSize; i++) {
      const r = Math.random();
      if (r <= inM1) {
        kv1.push([i, i + '']);
        removed.push(i);
      } else if (r <= inM1 + inM2) {
        kv2.push([i, i + '']);
        added.push(i);
      } else if (r <= inM1 + inM2 + inBoth) {
        kv1.push([i, i + '']);
        kv2.push([i, i + '_']);
        modified.push(i);
      } else {
        kv1.push([i, i + '']);
        kv2.push([i, i + '']);
      }
    }

    let m1 = new Map(kv1), m2 = new Map(kv2);

    if (m1.empty || m2.empty || added.length + removed.length + modified.length === 0) {
      return testRandomDiff(mapSize, inM1, inM2, inBoth);
    }

    const db = new TestDatabase();
    [m1, m2] = await Promise.all([m1, m2].map(s => db.readValue(db.writeValue(s).targetHash)));

    assert.deepEqual([[], [], []], await m1.diff(m1));
    assert.deepEqual([[], [], []], await m2.diff(m2));
    assert.deepEqual([removed, added, modified], await m1.diff(m2));
    assert.deepEqual([added, removed, modified], await m2.diff(m1));
    await db.close();
  }

  async function testSmallRandomDiff(inM1: number, inM2: number, inBoth: number) {
    const rounds = randomMapSize / smallRandomMapSize;
    for (let i = 0; i < rounds; i++) {
      await testRandomDiff(smallRandomMapSize, inM1, inM2, inBoth);
    }
  }

  test('LONG: random small map diff 0.1/0.1/0.1', () => testSmallRandomDiff(0.1, 0.1, 0.1));
  test('LONG: random small map diff 0.1/0.5/0.1', () => testSmallRandomDiff(0.1, 0.5, 0.1));
  test('LONG: random small map diff 0.1/0.1/0.5', () => testSmallRandomDiff(0.1, 0.1, 0.5));
  test('LONG: random small map diff 0.1/0.9/0', () => testSmallRandomDiff(0.1, 0.9, 0));

  test('LONG: random map diff 0.0001/0.0001/0.0001',
       () => testRandomDiff(randomMapSize, 0.0001, 0.0001, 0.0001));
  test('LONG: random map diff 0.0001/0.5/0.0001',
       () => testRandomDiff(randomMapSize, 0.0001, 0.5, 0.0001));
  test('LONG: random map diff 0.0001/0.0001/0.5',
       () => testRandomDiff(randomMapSize, 0.0001, 0.0001, 0.5));
  test('LONG: random map diff 0.0001/0.9999/0',
       () => testRandomDiff(randomMapSize, 0.0001, 0.9999, 0));

  test('LONG: random map diff 0.001/0.001/0.001',
       () => testRandomDiff(randomMapSize, 0.001, 0.001, 0.001));
  test('LONG: random map diff 0.001/0.5/0.001',
       () => testRandomDiff(randomMapSize, 0.001, 0.5, 0.001));
  test('LONG: random map diff 0.001/0.001/0.5',
       () => testRandomDiff(randomMapSize, 0.001, 0.001, 0.5));
  test('LONG: random map diff 0.001/0.999/0', () => testRandomDiff(randomMapSize, 0.001, 0.999, 0));

  test('LONG: random map diff 0.01/0.01/0.01',
       () => testRandomDiff(randomMapSize, 0.01, 0.01, 0.01));
  test('LONG: random map diff 0.01/0.5/0.1', () => testRandomDiff(randomMapSize, 0.01, 0.5, 0.1));
  test('LONG: random map diff 0.01/0.1/0.5', () => testRandomDiff(randomMapSize, 0.01, 0.1, 0.5));
  test('LONG: random map diff 0.01/0.99', () => testRandomDiff(randomMapSize, 0.01, 0.99, 0));

  test('LONG: random map diff 0.1/0.1/0.1', () => testRandomDiff(randomMapSize, 0.1, 0.1, 0.1));
  test('LONG: random map diff 0.1/0.5/0.1', () => testRandomDiff(randomMapSize, 0.1, 0.5, 0.1));
  test('LONG: random map diff 0.1/0.1/0.5', () => testRandomDiff(randomMapSize, 0.1, 0.1, 0.5));
  test('LONG: random map diff 0.1/0.9/0', () => testRandomDiff(randomMapSize, 0.1, 0.9, 0));

  test('chunks', () => {
    const m = build(db)[1];
    const chunks = m.chunks;
    const sequence = m.sequence;
    assert.equal(2, chunks.length);
    assert.deepEqual(sequence.items[0].ref, chunks[0]);
    assert.deepEqual(sequence.items[1].ref, chunks[1]);
  });

  test('Type after mutations', async () => {
    async function t(n, c) {
      const values: any = new Array(n);
      for (let i = 0; i < n; i++) {
        values[i] = [i, i];
      }

      let m = new Map(values);
      assert.equal(m.size, n);
      assert.instanceOf(m.sequence, c);
      assert.isTrue(equals(m.type, makeMapType(numberType, numberType)));

      m = await m.set('a', 'a');
      assert.equal(m.size, n + 1);
      assert.instanceOf(m.sequence, c);
      assert.isTrue(equals(m.type, makeMapType(makeUnionType([numberType, stringType]),
                                               makeUnionType([numberType, stringType]))));

      m = await m.delete('a');
      assert.equal(m.size, n);
      assert.instanceOf(m.sequence, c);
      assert.isTrue(equals(m.type, makeMapType(numberType, numberType)));
    }

    try {
      // This test shouldn't rely on a specific chunk size producing a leaf sequence, but it does.
      normalProductionChunks();
      await t(10, MapLeafSequence);
    } finally {
      smallTestChunks();
    }

    await t(1000, OrderedMetaSequence);
  });

  test('compound map with values of every type', async () => {
    const v = 42;
    const kvs = [
      // Values
      [true, v],
      [0, v],
      ['hello', v],
      [new Blob(new Uint8Array([0])), v],
      [new Set([true]), v],
      [new List([true]), v],
      [new Map([[true, true]]), v],
      [newStruct('', {field: true}), v],
      // Refs of values
      [new Ref(true), v],
      [new Ref(0), v],
      [new Ref('hello'), v],
      [new Ref(new Blob(new Uint8Array([0]))), v],
      [new Ref(new Set([true])), v],
      [new Ref(new List([true])), v],
      [new Ref(new Map([[true, true]])), v],
      [new Ref(newStruct('', {field: true})), v],
    ];

    let m = new Map(kvs);
    for (let i = 1; !m.sequence.isMeta; i++) {
      kvs.push([i, v]);
      m = await m.set(i, v);
    }

    assert.strictEqual(kvs.length, m.size);
    const [fk, fv] = notNull(await m.first());
    assert.strictEqual(true, fk);
    assert.strictEqual(v, fv);

    for (const kv of kvs) {
      assert.isTrue(await m.has(kv[0]));
      assert.strictEqual(v, await m.get(kv[0]));
      assert.strictEqual(v, kv[1]);
    }

    while (kvs.length > 0) {
      const kv = kvs.shift();
      m = await m.delete(kv[0]);
      assert.isFalse(await m.has(kv[0]));
      assert.strictEqual(kvs.length, m.size);
    }
  });

  test('Remove last when not loaded', async () => {
    const reload = async (m: Map<any, any>): Promise<Map<any, any>> => {
      const m2 = await db.readValue(db.writeValue(m).targetHash);
      invariant(m2 instanceof Map);
      return m2;
    };

    let kvs = intSequence(64).map(n => [n, n]);
    let map = new Map(kvs);

    while (kvs.length > 0) {
      const lastKey = kvs[kvs.length - 1][0];
      kvs = kvs.slice(0, kvs.length - 1);
      map = await map.delete(lastKey).then(reload);
      assert.isTrue(equals(new Map(kvs), map));
    }
  });
});
