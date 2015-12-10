// @flow

import {assert} from 'chai';
import {suite} from 'mocha';

import MemoryStore from './memory_store.js';
import test from './async_test.js';
import {CompoundList, ListLeaf} from './list.js';
import {Kind} from './noms_kind.js';
import {makeCompoundType, makePrimitiveType} from './type.js';
import {MetaTuple} from './meta_sequence.js';
import {writeValue} from './encode.js';

suite('ListLeaf', () => {
  test('get', async () => {
    let ms = new MemoryStore();
    let tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.String));
    let l = new ListLeaf(ms, tr, ['z', 'x', 'a', 'b']);
    assert.strictEqual('z', await l.get(0));
    assert.strictEqual('x', await l.get(1));
    assert.strictEqual('a', await l.get(2));
    assert.strictEqual('b', await l.get(3));
  });

  test('forEach', async () => {
    let ms = new MemoryStore();
    let tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int32));
    let l = new ListLeaf(ms, tr, [4, 2, 10, 16]);

    let values = [];
    await l.forEach((v, i) => { values.push(v, i); });
    assert.deepEqual([4, 0, 2, 1, 10, 2, 16, 3], values);
  });
});

suite('CompoundList', () => {
  function build(): CompoundList {
    let ms = new MemoryStore();
    let tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.String));
    let l1 = new ListLeaf(ms, tr, ['a', 'b']);
    let r1 = writeValue(l1, tr, ms);
    let l2 = new ListLeaf(ms, tr, ['e', 'f']);
    let r2 = writeValue(l2, tr, ms);
    let l3 = new ListLeaf(ms, tr, ['h', 'i']);
    let r3 = writeValue(l3, tr, ms);
    let l4 = new ListLeaf(ms, tr, ['m', 'n']);
    let r4 = writeValue(l4, tr, ms);

    let m1 = new CompoundList(ms, tr, [new MetaTuple(r1, 2), new MetaTuple(r2, 2)]);
    let rm1 = writeValue(m1, tr, ms);
    let m2 = new CompoundList(ms, tr, [new MetaTuple(r3, 2), new MetaTuple(r4, 2)]);
    let rm2 = writeValue(m2, tr, ms);

    let l = new CompoundList(ms, tr, [new MetaTuple(rm1, 4), new MetaTuple(rm2, 4)]);
    return l;
  }

  test('get', async () => {
    let l = build();
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
    let l = build();
    let values = [];
    await l.forEach((k, i) => { values.push(k, i); });
    assert.deepEqual(['a', 0, 'b', 1, 'e', 2, 'f', 3, 'h', 4, 'i', 5, 'm', 6, 'n', 7], values);
  });
});
