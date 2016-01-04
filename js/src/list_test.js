// @flow

import {assert} from 'chai';
import {suite} from 'mocha';

import MemoryStore from './memory_store.js';
import test from './async_test.js';
import {IndexedMetaSequence, MetaTuple} from './meta_sequence.js';
import {Kind} from './noms_kind.js';
import {ListLeafSequence, NomsList} from './list.js';
import {makeCompoundType, makePrimitiveType} from './type.js';
import {writeValue} from './encode.js';

suite('ListLeafSequence', () => {
  test('get', async () => {
    let ms = new MemoryStore();
    let tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.String));
    let l = new NomsList(ms, tr, new ListLeafSequence(tr, ['z', 'x', 'a', 'b']));
    assert.strictEqual('z', await l.get(0));
    assert.strictEqual('x', await l.get(1));
    assert.strictEqual('a', await l.get(2));
    assert.strictEqual('b', await l.get(3));
  });

  test('forEach', async () => {
    let ms = new MemoryStore();
    let tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Int32));
    let l = new NomsList(ms, tr, new ListLeafSequence(tr, [4, 2, 10, 16]));

    let values = [];
    await l.forEach((v, i) => { values.push(v, i); });
    assert.deepEqual([4, 0, 2, 1, 10, 2, 16, 3], values);
  });

  test('chunks', () => {
    let ms = new MemoryStore();
    let tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.Value));
    let st = makePrimitiveType(Kind.String);
    let r1 = writeValue('x', st, ms);
    let r2 = writeValue('a', st, ms);
    let r3 = writeValue('b', st, ms);
    let l = new NomsList(ms, tr, new ListLeafSequence(tr, ['z', r1, r2, r3]));
    assert.strictEqual(3, l.chunks.length);
    assert.isTrue(r1.equals(l.chunks[0]));
    assert.isTrue(r2.equals(l.chunks[1]));
    assert.isTrue(r3.equals(l.chunks[2]));
  });
});

suite('CompoundList', () => {
  function build(): NomsList {
    let ms = new MemoryStore();
    let tr = makeCompoundType(Kind.List, makePrimitiveType(Kind.String));
    let l1 = new NomsList(ms, tr, new ListLeafSequence(tr, ['a', 'b']));
    let r1 = writeValue(l1, tr, ms);
    let l2 = new NomsList(ms, tr, new ListLeafSequence(tr, ['e', 'f']));
    let r2 = writeValue(l2, tr, ms);
    let l3 = new NomsList(ms, tr, new ListLeafSequence(tr, ['h', 'i']));
    let r3 = writeValue(l3, tr, ms);
    let l4 = new NomsList(ms, tr, new ListLeafSequence(tr, ['m', 'n']));
    let r4 = writeValue(l4, tr, ms);

    let m1 = new NomsList(ms, tr, new IndexedMetaSequence(tr, [new MetaTuple(r1, 2), new MetaTuple(r2, 2)]));
    let rm1 = writeValue(m1, tr, ms);
    let m2 = new NomsList(ms, tr, new IndexedMetaSequence(tr, [new MetaTuple(r3, 2), new MetaTuple(r4, 2)]));
    let rm2 = writeValue(m2, tr, ms);

    let l = new NomsList(ms, tr, new IndexedMetaSequence(tr, [new MetaTuple(rm1, 4), new MetaTuple(rm2, 4)]));
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

  test('chunks', () => {
    let l = build();
    assert.strictEqual(2, l.chunks.length);
  });
});
