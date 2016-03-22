// @flow

import {assert} from 'chai';
import {suite} from 'mocha';

import MemoryStore from './memory-store.js';
import test from './async-test.js';
import {invariant} from './assert.js';
import {Kind} from './noms-kind.js';
import {flatten, flattenParallel} from './test-util.js';
import {makeCompoundType, makePrimitiveType} from './type.js';
import {MapLeafSequence, newMap, NomsMap} from './map.js';
import {MetaTuple, OrderedMetaSequence} from './meta-sequence.js';
import DataStore from './data-store';

const testMapSize = 5000;
const mapOfNRef = 'sha1-1b9664e55091370996f3af428ffee78f1ad36426';

suite('BuildMap', () => {
  test('set of n numbers', async () => {
    const kvs = [];
    for (let i = 0; i < testMapSize; i++) {
      kvs.push(i, i + 1);
    }

    const tr = makeCompoundType(Kind.Map, makePrimitiveType(Kind.Int64),
                                makePrimitiveType(Kind.Int64));
    const m = await newMap(kvs, tr);
    assert.strictEqual(m.ref.toString(), mapOfNRef);

    // shuffle kvs, and test that the constructor sorts properly
    const pairs = [];
    for (let i = 0; i < kvs.length; i += 2) {
      pairs.push({k: kvs[i], v: kvs[i + 1]});
    }
    pairs.sort(() => Math.random() > .5 ? 1 : -1);
    kvs.length = 0;
    pairs.forEach(kv => kvs.push(kv.k, kv.v));
    const m2 = await newMap(kvs, tr);
    assert.strictEqual(m2.ref.toString(), mapOfNRef);
  });

  test('set', async () => {
    const kvs = [];
    for (let i = 0; i < testMapSize - 10; i++) {
      kvs.push(i, i + 1);
    }

    const tr = makeCompoundType(Kind.Map, makePrimitiveType(Kind.Int64),
                                makePrimitiveType(Kind.Int64));
    let m = await newMap(kvs, tr);
    for (let i = testMapSize - 10; i < testMapSize; i++) {
      m = await m.set(i, i + 1);
    }

    assert.strictEqual(m.ref.toString(), mapOfNRef);
  });

  test('set existing', async () => {
    const kvs = [];
    for (let i = 0; i < testMapSize; i++) {
      kvs.push(i, i + 1);
    }

    const tr = makeCompoundType(Kind.Map, makePrimitiveType(Kind.Int64),
                                makePrimitiveType(Kind.Int64));
    let m = await newMap(kvs, tr);
    for (let i = 0; i < testMapSize; i++) {
      m = await m.set(i, i + 1);
    }

    assert.strictEqual(m.ref.toString(), mapOfNRef);
  });

  test('remove', async () => {
    const kvs = [];
    for (let i = 0; i < testMapSize + 10; i++) {
      kvs.push(i, i + 1);
    }

    const tr = makeCompoundType(Kind.Map, makePrimitiveType(Kind.Int64),
                                makePrimitiveType(Kind.Int64));
    let m = await newMap(kvs, tr);
    for (let i = testMapSize; i < testMapSize + 10; i++) {
      m = await m.remove(i);
    }

    assert.strictEqual(m.ref.toString(), mapOfNRef);
  });

  test('write, read, modify, read', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);

    const kvs = [];
    for (let i = 0; i < testMapSize; i++) {
      kvs.push(i, i + 1);
    }

    const tr = makeCompoundType(Kind.Map, makePrimitiveType(Kind.Int64),
                                makePrimitiveType(Kind.Int64));
    const m = await newMap(kvs, tr);

    const r = ds.writeValue(m);
    const m2 = await ds.readValue(r);
    const outKvs = [];
    await m2.forEach((v, k) => outKvs.push(k, v));
    assert.deepEqual(kvs, outKvs);

    invariant(m2 instanceof NomsMap);
    const m3 = await m2.remove(testMapSize - 1);
    const outKvs2 = [];
    await m3.forEach((v, k) => outKvs2.push(k, v));
    kvs.splice(testMapSize * 2 - 2, 2);
    assert.deepEqual(kvs, outKvs2);
  });
});

suite('MapLeaf', () => {
  test('isEmpty', () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const tr = makeCompoundType(Kind.Map, makePrimitiveType(Kind.String),
                                makePrimitiveType(Kind.Bool));
    const newMap = entries => new NomsMap(tr, new MapLeafSequence(ds, tr, entries));
    assert.isTrue(newMap([]).isEmpty());
    assert.isFalse(newMap([{key: 'a', value: false}, {key:'k', value:true}]).isEmpty());
  });

  test('has', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const tr = makeCompoundType(Kind.Map, makePrimitiveType(Kind.String),
                                makePrimitiveType(Kind.Bool));
    const m = new NomsMap(tr,
        new MapLeafSequence(ds, tr, [{key: 'a', value: false}, {key:'k', value:true}]));
    assert.isTrue(await m.has('a'));
    assert.isFalse(await m.has('b'));
    assert.isTrue(await m.has('k'));
    assert.isFalse(await m.has('z'));
  });

  test('first/last/get', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const tr = makeCompoundType(Kind.Map, makePrimitiveType(Kind.String),
                                makePrimitiveType(Kind.Int32));
    const m = new NomsMap(tr,
        new MapLeafSequence(ds, tr, [{key: 'a', value: 4}, {key:'k', value:8}]));

    assert.deepEqual(['a', 4], await m.first());
    assert.deepEqual(['k', 8], await m.last());

    assert.strictEqual(4, await m.get('a'));
    assert.strictEqual(undefined, await m.get('b'));
    assert.strictEqual(8, await m.get('k'));
    assert.strictEqual(undefined, await m.get('z'));
  });

  test('forEach', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const tr = makeCompoundType(Kind.Map, makePrimitiveType(Kind.String),
                                makePrimitiveType(Kind.Int32));
    const m = new NomsMap(tr,
        new MapLeafSequence(ds, tr, [{key: 'a', value: 4}, {key:'k', value:8}]));

    const kv = [];
    await m.forEach((v, k) => { kv.push(k, v); });
    assert.deepEqual(['a', 4, 'k', 8], kv);
  });

  test('iterator', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const tr = makeCompoundType(Kind.Map, makePrimitiveType(Kind.String),
                                makePrimitiveType(Kind.Int32));

    const test = async entries => {
      const m = new NomsMap(tr, new MapLeafSequence(ds, tr, entries));
      assert.deepEqual(entries, await flatten(m.iterator()));
      assert.deepEqual(entries, await flattenParallel(m.iterator(), entries.length));
    };

    await test([]);
    await test([{key: 'a', value: 4}]);
    await test([{key: 'a', value: 4}, {key: 'k', value: 8}]);
  });

  test('iteratorAt', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const tr = makeCompoundType(Kind.Map, makePrimitiveType(Kind.String),
                                makePrimitiveType(Kind.Int32));
    const build = entries => new NomsMap(tr, new MapLeafSequence(ds, tr, entries));

    assert.deepEqual([], await flatten(build([]).iteratorAt('a')));

    {
      const kv = [{key: 'b', value: 5}];
      assert.deepEqual(kv, await flatten(build(kv).iteratorAt('a')));
      assert.deepEqual(kv, await flatten(build(kv).iteratorAt('b')));
      assert.deepEqual([], await flatten(build(kv).iteratorAt('c')));
    }

    {
      const kv = [{key: 'b', value: 5}, {key: 'd', value: 10}];
      assert.deepEqual(kv, await flatten(build(kv).iteratorAt('a')));
      assert.deepEqual(kv, await flatten(build(kv).iteratorAt('b')));
      assert.deepEqual(kv.slice(1), await flatten(build(kv).iteratorAt('c')));
      assert.deepEqual(kv.slice(1), await flatten(build(kv).iteratorAt('d')));
      assert.deepEqual([], await flatten(build(kv).iteratorAt('e')));
    }
  });

  test('chunks', () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const tr = makeCompoundType(Kind.Map,
                                makePrimitiveType(Kind.Value), makePrimitiveType(Kind.Value));
    const r1 = ds.writeValue('x');
    const r2 = ds.writeValue('a');
    const r3 = ds.writeValue('b');
    const r4 = ds.writeValue('c');
    const m = new NomsMap(tr,
        new MapLeafSequence(ds, tr, [{key: r1, value: r2}, {key: r3, value: r4}]));
    assert.strictEqual(4, m.chunks.length);
    assert.isTrue(r1.equals(m.chunks[0]));
    assert.isTrue(r2.equals(m.chunks[1]));
    assert.isTrue(r3.equals(m.chunks[2]));
    assert.isTrue(r4.equals(m.chunks[3]));
  });
});

suite('CompoundMap', () => {
  function build(ds: DataStore): Array<NomsMap> {
    const tr = makeCompoundType(Kind.Map, makePrimitiveType(Kind.String),
        makePrimitiveType(Kind.Bool));
    const l1 = new NomsMap(tr, new MapLeafSequence(ds, tr, [{key: 'a', value: false},
        {key:'b', value:false}]));
    const r1 = ds.writeValue(l1);
    const l2 = new NomsMap(tr, new MapLeafSequence(ds, tr, [{key: 'e', value: true},
        {key:'f', value:true}]));
    const r2 = ds.writeValue(l2);
    const l3 = new NomsMap(tr, new MapLeafSequence(ds, tr, [{key: 'h', value: false},
        {key:'i', value:true}]));
    const r3 = ds.writeValue(l3);
    const l4 = new NomsMap(tr, new MapLeafSequence(ds, tr, [{key: 'm', value: true},
        {key:'n', value:false}]));
    const r4 = ds.writeValue(l4);

    const m1 = new NomsMap(tr, new OrderedMetaSequence(ds, tr, [new MetaTuple(r1, 'b'),
        new MetaTuple(r2, 'f')]));
    const rm1 = ds.writeValue(m1);
    const m2 = new NomsMap(tr, new OrderedMetaSequence(ds, tr, [new MetaTuple(r3, 'i'),
        new MetaTuple(r4, 'n')]));
    const rm2 = ds.writeValue(m2);

    const c = new NomsMap(tr, new OrderedMetaSequence(ds, tr, [new MetaTuple(rm1, 'f'),
        new MetaTuple(rm2, 'n')]));
    return [c, m1, m2];
  }

  test('isEmpty', () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const [c] = build(ds);
    assert.isFalse(c.isEmpty());
  });

  test('get', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const [c] = build(ds);

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
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const [c, m1, m2] = build(ds);

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
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const [c] = build(ds);

    const kv = [];
    await c.forEach((v, k) => { kv.push(k, v); });
    assert.deepEqual(['a', false, 'b', false, 'e', true, 'f', true, 'h', false, 'i', true, 'm',
        true, 'n', false], kv);
  });

  test('iterator', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const [c] = build(ds);
    const expected = [{key: 'a', value: false}, {key: 'b', value: false}, {key: 'e', value: true},
                      {key: 'f', value: true}, {key: 'h', value: false}, {key: 'i', value: true},
                      {key: 'm', value: true}, {key: 'n', value: false}];
    assert.deepEqual(expected, await flatten(c.iterator()));
    assert.deepEqual(expected, await flattenParallel(c.iterator(), expected.length));
  });

  test('iteratorAt', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const [c] = build(ds);
    const entries = [{key: 'a', value: false}, {key: 'b', value: false}, {key: 'e', value: true},
                     {key: 'f', value: true}, {key: 'h', value: false}, {key: 'i', value: true},
                     {key: 'm', value: true}, {key: 'n', value: false}];
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
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const [c] = build(ds);
    const iter = c.iterator();
    const values = [];
    for (let res = await iter.next(); !res.done; res = await iter.next()) {
      values.push(res.value);
      if (values.length === 5) {
        await iter.return();
      }
    }
    assert.deepEqual([{key: 'a', value: false}, {key: 'b', value: false}, {key: 'e', value: true},
                      {key: 'f', value: true}, {key: 'h', value: false}],
                     values);
  });

  test('iterator return parallel', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const [c] = build(ds);
    const iter = c.iterator();
    const values = await Promise.all([iter.next(), iter.next(), iter.return(), iter.next()]);
    assert.deepEqual([{done: false, value: {key: 'a', value: false}},
                      {done: false, value: {key: 'b', value: false}},
                      {done: true}, {done: true}],
                     values);
  });

  test('chunks', () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const [c] = build(ds);
    assert.strictEqual(2, c.chunks.length);
  });
});
