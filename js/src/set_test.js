// @flow

import {assert} from 'chai';
import {suite} from 'mocha';

import MemoryStore from './memory_store.js';
import test from './async_test.js';
import type {ChunkStore} from './chunk_store.js';
import {invariant, notNull} from './assert.js';
import {Kind} from './noms_kind.js';
import {flatten} from './test_util.js';
import {makeCompoundType, makePrimitiveType} from './type.js';
import {MetaTuple, OrderedMetaSequence} from './meta_sequence.js';
import {newSet, NomsSet, SetLeafSequence} from './set.js';
import {OrderedSequence} from './ordered_sequence.js';
import {writeValue} from './encode.js';

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
    const ms = new MemoryStore();
    const nums = firstNNumbers(testSetSize);
    const tr = makeCompoundType(Kind.Set, makePrimitiveType(Kind.Int64));
    const s = await newSet(ms, tr, nums);
    assert.strictEqual(s.ref.toString(), setOfNRef);

    // shuffle kvs, and test that the constructor sorts properly
    nums.sort(() => Math.random() > .5 ? 1 : -1);
    const s2 = await newSet(ms, tr, nums);
    assert.strictEqual(s2.ref.toString(), setOfNRef);
  });

  test('insert', async () => {
    const ms = new MemoryStore();
    const nums = firstNNumbers(testSetSize - 10);
    const tr = makeCompoundType(Kind.Set, makePrimitiveType(Kind.Int64));
    let s = await newSet(ms, tr, nums);

    for (let i = testSetSize - 10; i < testSetSize; i++) {
      s = await s.insert(i);
    }

    assert.strictEqual(s.ref.toString(), setOfNRef);
  });

  test('remove', async () => {
    const ms = new MemoryStore();
    const nums = firstNNumbers(testSetSize + 10);
    const tr = makeCompoundType(Kind.Set, makePrimitiveType(Kind.Int64));
    let s = await newSet(ms, tr, nums);

    let count = 10;
    while (count-- > 0) {
      s = await s.remove(testSetSize + count);
    }

    assert.strictEqual(s.ref.toString(), setOfNRef);
  });

});

suite('SetLeaf', () => {
  test('isEmpty', () => {
    const ms = new MemoryStore();
    const tr = makeCompoundType(Kind.Set, makePrimitiveType(Kind.String));
    const newSet = items => new NomsSet(ms, tr, new SetLeafSequence(tr, items));
    assert.isTrue(newSet([]).isEmpty());
    assert.isFalse(newSet(['a', 'k']).isEmpty());
  });

  test('first/has', async () => {
    const ms = new MemoryStore();
    const tr = makeCompoundType(Kind.Set, makePrimitiveType(Kind.String));
    const s = new NomsSet(ms, tr, new SetLeafSequence(tr, ['a', 'k']));

    assert.strictEqual('a', await s.first());

    assert.isTrue(await s.has('a'));
    assert.isFalse(await s.has('b'));
    assert.isTrue(await s.has('k'));
    assert.isFalse(await s.has('z'));
  });

  test('forEach', async () => {
    const ms = new MemoryStore();
    const tr = makeCompoundType(Kind.Set, makePrimitiveType(Kind.String));
    const m = new NomsSet(ms, tr, new SetLeafSequence(tr, ['a', 'b']));

    const values = [];
    await m.forEach((k) => { values.push(k); });
    assert.deepEqual(['a', 'b'], values);
  });

  test('iterator', async () => {
    const ms = new MemoryStore();
    const tr = makeCompoundType(Kind.Set, makePrimitiveType(Kind.String));

    const test = async items => {
      const m = new NomsSet(ms, tr, new SetLeafSequence(tr, items));
      assert.deepEqual(items, await flatten(m.iterator()));
    };

    await test([]);
    await test(['a']);
    await test(['a', 'b']);
  });

  test('iteratorAt', async () => {
    const ms = new MemoryStore();
    const tr = makeCompoundType(Kind.Set, makePrimitiveType(Kind.String));
    const build = items => new NomsSet(ms, tr, new SetLeafSequence(tr, items));

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

  test('chunks', () => {
    const ms = new MemoryStore();
    const tr = makeCompoundType(Kind.Set, makePrimitiveType(Kind.Value));
    const st = makePrimitiveType(Kind.String);
    const r1 = writeValue('x', st, ms);
    const r2 = writeValue('a', st, ms);
    const r3 = writeValue('b', st, ms);
    const l = new NomsSet(ms, tr, new SetLeafSequence(tr, ['z', r1, r2, r3]));
    assert.strictEqual(3, l.chunks.length);
    assert.isTrue(r1.equals(l.chunks[0]));
    assert.isTrue(r2.equals(l.chunks[1]));
    assert.isTrue(r3.equals(l.chunks[2]));
  });
});

suite('CompoundSet', () => {
  function build(cs: ChunkStore, values: Array<string>): NomsSet {
    const tr = makeCompoundType(Kind.Set, makePrimitiveType(Kind.String));
    assert.isTrue(values.length > 1 && Math.log2(values.length) % 1 === 0);

    let tuples = [];
    for (let i = 0; i < values.length; i += 2) {
      const l = new NomsSet(cs, tr, new SetLeafSequence(tr, [values[i], values[i + 1]]));
      const r = writeValue(l, tr, cs);
      tuples.push(new MetaTuple(r, values[i + 1]));
    }

    let last: ?NomsSet = null;
    while (tuples.length > 1) {
      const next = [];
      for (let i = 0; i < tuples.length; i += 2) {
        last = new NomsSet(cs, tr, new OrderedMetaSequence(tr, [tuples[i], tuples[i + 1]]));
        const r = writeValue(last, tr, cs);
        next.push(new MetaTuple(r, tuples[i + 1].value));
      }

      tuples = next;
    }

    return notNull(last);
  }

  test('isEmpty', () => {
    const ms = new MemoryStore();
    const c = build(ms, ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n']);
    assert.isFalse(c.isEmpty());
  });

  test('first/has', async () => {
    const ms = new MemoryStore();
    const c = build(ms, ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n']);
    assert.strictEqual('a', await c.first());
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
    const c = build(ms, ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n']);
    const values = [];
    await c.forEach((k) => { values.push(k); });
    assert.deepEqual(['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n'], values);
  });

  test('iterator', async () => {
    const ms = new MemoryStore();
    const c = build(ms, ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n']);
    assert.deepEqual(['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n'], await flatten(c.iterator()));
  });

  test('iteratorAt', async () => {
    const ms = new MemoryStore();
    const values = ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n'];
    const c = build(ms, values);
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
      assert.deepEqual(values.slice(offsets[k]), await flatten(c.iteratorAt(k)));
    }
  });

  test('iterator return', async () => {
    const ms = new MemoryStore();
    const values = ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n'];
    const c = build(ms, values);
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

  test('chunks', () => {
    const ms = new MemoryStore();
    const c = build(ms, ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n']);
    assert.strictEqual(2, c.chunks.length);
  });

  test('map', async () => {
    const ms = new MemoryStore();
    const c = build(ms, ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n']);
    const values = await c.map((k) => k + '*');
    assert.deepEqual(['a*', 'b*', 'e*', 'f*', 'h*', 'i*', 'm*', 'n*'], values);
  });

  test('map async', async () => {
    const ms = new MemoryStore();
    const c = build(ms, ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n']);
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

    const c = build(ms, ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n']);

    invariant(c.sequence instanceof OrderedSequence);
    let cursor = await c.sequence.newCursorAt(c.cs, null);
    assert.ok(cursor);
    assert.strictEqual('a', cursor.getCurrent());

    assert.isTrue(await cursor.advanceTo('h'));
    assert.strictEqual('h', cursor.getCurrent());

    assert.isTrue(await cursor.advanceTo('k'));
    assert.strictEqual('m', cursor.getCurrent());

    assert.isFalse(await cursor.advanceTo('z')); // not found
    assert.isFalse(cursor.valid);

    invariant(c.sequence instanceof OrderedSequence);
    cursor = await c.sequence.newCursorAt(ms, 'x'); // not found
    assert.isFalse(cursor.valid);

    invariant(c.sequence instanceof OrderedSequence);
    cursor = await c.sequence.newCursorAt(ms, 'e');
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

    const first = build(ms, seqs[0]);
    const sets:Array<NomsSet> = [];
    for (let i = 1; i < seqs.length; i++) {
      sets.push(build(ms, seqs[i]));
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
});
