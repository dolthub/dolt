// @flow

import {assert} from 'chai';
import {suite} from 'mocha';

import MemoryStore from './memory_store.js';
import test from './async_test.js';
import {flatten} from './test_util.js';
import {IndexedMetaSequence, MetaTuple} from './meta_sequence.js';
import {Kind} from './noms_kind.js';
import {ListLeafSequence, newList, NomsList} from './list.js';
import {makeCompoundType, makePrimitiveType} from './type.js';
import {writeValue} from './encode.js';

const testListSize = 5000;
const listOfNRef = 'sha1-11e947e8aacfda8e9052bb57e661da442b26c625';

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

  test('set of n numbers', async () => {
    const ms = new MemoryStore();
    const nums = firstNNumbers(testListSize);
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int64));
    const s = await newList(ms, tr, nums);
    assert.strictEqual(s.ref.toString(), listOfNRef);
  });

  test('insert', async () => {
    const ms = new MemoryStore();
    const nums = firstNNumbers(testListSize - 10);
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int64));
    let s = await newList(ms, tr, nums);

    for (let i = testListSize - 10; i < testListSize; i++) {
      s = await s.insert(i, [i]);
    }

    assert.strictEqual(s.ref.toString(), listOfNRef);
  });

  test('append', async () => {
    const ms = new MemoryStore();
    const nums = firstNNumbers(testListSize - 10);
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int64));
    let s = await newList(ms, tr, nums);

    for (let i = testListSize - 10; i < testListSize; i++) {
      s = await s.append([i]);
    }

    assert.strictEqual(s.ref.toString(), listOfNRef);
  });

  test('remove', async () => {
    const ms = new MemoryStore();
    const nums = firstNNumbers(testListSize + 10);
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int64));
    let s = await newList(ms, tr, nums);

    let count = 10;
    while (count-- > 0) {
      s = await s.remove(testListSize + count, testListSize + count + 1);
    }

    assert.strictEqual(s.ref.toString(), listOfNRef);
  });

  test('splice', async () => {
    const ms = new MemoryStore();
    const nums = firstNNumbers(testListSize);
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int64));
    let s = await newList(ms, tr, nums);

    const splice500At = async (idx: number) => {
      s = await s.splice(idx, [], 500);
      s = await s.splice(idx, intSequence(idx, idx + 500), 0);
    };


    for (let i = 0; i < testListSize / 1000; i++) {
      await splice500At(i * 1000);
    }

    assert.strictEqual(s.ref.toString(), listOfNRef);
  });
});

suite('ListLeafSequence', () => {
  test('isEmpty', () => {
    const ms = new MemoryStore();
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.String));
    const newList = items => new NomsList(ms, tr, new ListLeafSequence(tr, items));
    assert.isTrue(newList([]).isEmpty());
    assert.isFalse(newList(['z', 'x', 'a', 'b']).isEmpty());
  });

  test('get', async () => {
    const ms = new MemoryStore();
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.String));
    const l = new NomsList(ms, tr, new ListLeafSequence(tr, ['z', 'x', 'a', 'b']));
    assert.strictEqual('z', await l.get(0));
    assert.strictEqual('x', await l.get(1));
    assert.strictEqual('a', await l.get(2));
    assert.strictEqual('b', await l.get(3));
  });

  test('forEach', async () => {
    const ms = new MemoryStore();
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int32));
    const l = new NomsList(ms, tr, new ListLeafSequence(tr, [4, 2, 10, 16]));

    const values = [];
    await l.forEach((v, i) => { values.push(v, i); });
    assert.deepEqual([4, 0, 2, 1, 10, 2, 16, 3], values);
  });

  test('iterator', async () => {
    const ms = new MemoryStore();
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int32));

    const test = async items => {
      const l = new NomsList(ms, tr, new ListLeafSequence(tr, items));
      assert.deepEqual(items, await flatten(l.iterator()));
    };

    await test([]);
    await test([42]);
    await test([4, 2, 10, 16]);
  });

  test('iteratorAt', async () => {
    const ms = new MemoryStore();
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int32));

    const test = async items => {
      const l = new NomsList(ms, tr, new ListLeafSequence(tr, items));
      for (let i = 0; i <= items.length; i++) {
        assert.deepEqual(items.slice(i), await flatten(l.iteratorAt(i)));
      }
    };

    await test([]);
    await test([42]);
    await test([4, 2, 10, 16]);
  });

  test('chunks', () => {
    const ms = new MemoryStore();
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Value));
    const st = makePrimitiveType(Kind.String);
    const r1 = writeValue('x', st, ms);
    const r2 = writeValue('a', st, ms);
    const r3 = writeValue('b', st, ms);
    const l = new NomsList(ms, tr, new ListLeafSequence(tr, ['z', r1, r2, r3]));
    assert.strictEqual(3, l.chunks.length);
    assert.isTrue(r1.equals(l.chunks[0]));
    assert.isTrue(r2.equals(l.chunks[1]));
    assert.isTrue(r3.equals(l.chunks[2]));
  });
});

suite('CompoundList', () => {
  function build(): NomsList {
    const ms = new MemoryStore();
    const tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.String));
    const l1 = new NomsList(ms, tr, new ListLeafSequence(tr, ['a', 'b']));
    const r1 = writeValue(l1, tr, ms);
    const l2 = new NomsList(ms, tr, new ListLeafSequence(tr, ['e', 'f']));
    const r2 = writeValue(l2, tr, ms);
    const l3 = new NomsList(ms, tr, new ListLeafSequence(tr, ['h', 'i']));
    const r3 = writeValue(l3, tr, ms);
    const l4 = new NomsList(ms, tr, new ListLeafSequence(tr, ['m', 'n']));
    const r4 = writeValue(l4, tr, ms);

    const m1 = new NomsList(ms, tr, new IndexedMetaSequence(tr, [new MetaTuple(r1, 2),
        new MetaTuple(r2, 2)]));
    const rm1 = writeValue(m1, tr, ms);
    const m2 = new NomsList(ms, tr, new IndexedMetaSequence(tr, [new MetaTuple(r3, 2),
        new MetaTuple(r4, 2)]));
    const rm2 = writeValue(m2, tr, ms);

    const l = new NomsList(ms, tr, new IndexedMetaSequence(tr, [new MetaTuple(rm1, 4),
        new MetaTuple(rm2, 4)]));
    return l;
  }

  test('isEmpty', () => {
    assert.isFalse(build().isEmpty());
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
    assert.deepEqual(['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n'], await flatten(build().iterator()));
  });

  test('iteratorAt', async () => {
    const values = ['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n'];
    for (let i = 0; i <= values.length; i++) {
      assert.deepEqual(values.slice(i), await flatten(build().iteratorAt(i)));
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

  test('chunks', () => {
    const l = build();
    assert.strictEqual(2, l.chunks.length);
  });

  test('length', () => {
    const l = build();
    assert.equal(l.length, 8);
  });
});
