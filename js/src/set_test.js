// @flow

import {assert} from 'chai';
import {suite} from 'mocha';

import MemoryStore from './memory_store.js';
import test from './async_test.js';
import {CompoundSet, SetLeaf} from './set.js';
import {notNull} from './assert.js';
import {Kind} from './noms_kind.js';
import {makeCompoundType, makePrimitiveType} from './type.js';
import {MetaTuple} from './meta_sequence.js';
import {writeValue} from './encode.js';

suite('SetLeaf', () => {
  test('first/has', async () => {
    let ms = new MemoryStore();
    let tr = makeCompoundType(Kind.Set, makePrimitiveType(Kind.String));
    let s = new SetLeaf(ms, tr, ['a', 'k']);

    assert.strictEqual('a', await s.first());

    assert.isTrue(await s.has('a'));
    assert.isFalse(await s.has('b'));
    assert.isTrue(await s.has('k'));
    assert.isFalse(await s.has('z'));
  });

  test('forEach', async () => {
    let ms = new MemoryStore();
    let tr = makeCompoundType(Kind.Set, makePrimitiveType(Kind.String));
    let m = new SetLeaf(ms, tr, ['a', 'b']);

    let values = [];
    await m.forEach((k) => { values.push(k); });
    assert.deepEqual(['a', 'b'], values);
  });
});

suite('CompoundSet', () => {
  function build(): Array<CompoundSet> {
    let ms = new MemoryStore();
    let tr = makeCompoundType(Kind.Set, makePrimitiveType(Kind.String));
    let l1 = new SetLeaf(ms, tr, ['a', 'b']);
    let r1 = writeValue(l1, tr, ms);
    let l2 = new SetLeaf(ms, tr, ['e', 'f']);
    let r2 = writeValue(l2, tr, ms);
    let l3 = new SetLeaf(ms, tr, ['h', 'i']);
    let r3 = writeValue(l3, tr, ms);
    let l4 = new SetLeaf(ms, tr, ['m', 'n']);
    let r4 = writeValue(l4, tr, ms);

    let m1 = new CompoundSet(ms, tr, [new MetaTuple(r1, 'b'), new MetaTuple(r2, 'f')]);
    let rm1 = writeValue(m1, tr, ms);
    let m2 = new CompoundSet(ms, tr, [new MetaTuple(r3, 'i'), new MetaTuple(r4, 'n')]);
    let rm2 = writeValue(m2, tr, ms);

    let c = new CompoundSet(ms, tr, [new MetaTuple(rm1, 'f'), new MetaTuple(rm2, 'n')]);
    return [c, m1, m2];
  }

  test('first/has', async () => {
    let [c, m1, m2] = build();
    assert.strictEqual('a', await m1.first());
    assert.strictEqual('h', await m2.first());
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
    let [c] = build();

    let values = [];
    await c.forEach((k) => { values.push(k); });
    assert.deepEqual(['a', 'b', 'e', 'f', 'h', 'i', 'm', 'n'], values);
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
    let [c] = build();

    let cursor = await c.newCursorAt(null);
    assert.ok(cursor);
    assert.strictEqual('a', cursor.getCurrent());

    assert.isTrue(await cursor.advanceTo('h'));
    assert.strictEqual('h', cursor.getCurrent());

    assert.isTrue(await cursor.advanceTo('k'));
    assert.strictEqual('m', cursor.getCurrent());

    assert.isFalse(await cursor.advanceTo('z')); // not found
    assert.isFalse(cursor.valid);

    cursor = await c.newCursorAt('x'); // not found
    assert.isFalse(cursor.valid);

    cursor = await c.newCursorAt('e');
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
});

