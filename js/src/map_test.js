// @flow

import {assert} from 'chai';
import {suite} from 'mocha';

import MemoryStore from './memory_store.js';
import test from './async_test.js';
import {CompoundMap, MapLeaf} from './map.js';
import {Kind} from './noms_kind.js';
import {makeCompoundType, makePrimitiveType} from './type.js';
import {MetaTuple} from './meta_sequence.js';
import {writeValue} from './encode.js';

suite('MapLeaf', () => {
  test('has', async () => {
    let ms = new MemoryStore();
    let tr = makeCompoundType(Kind.Map, makePrimitiveType(Kind.String), makePrimitiveType(Kind.Bool));
    let m = new MapLeaf(ms, tr, [{key: 'a', value: false}, {key:'k', value:true}]);
    assert.isTrue(await m.has('a'));
    assert.isFalse(await m.has('b'));
    assert.isTrue(await m.has('k'));
    assert.isFalse(await m.has('z'));
  });

  test('first/get', async () => {
    let ms = new MemoryStore();
    let tr = makeCompoundType(Kind.Map, makePrimitiveType(Kind.String), makePrimitiveType(Kind.Int32));
    let m = new MapLeaf(ms, tr, [{key: 'a', value: 4}, {key:'k', value:8}]);

    assert.deepEqual(['a', 4], await m.first());

    assert.strictEqual(4, await m.get('a'));
    assert.strictEqual(undefined, await m.get('b'));
    assert.strictEqual(8, await m.get('k'));
    assert.strictEqual(undefined, await m.get('z'));
  });

  test('forEach', async () => {
    let ms = new MemoryStore();
    let tr = makeCompoundType(Kind.Map, makePrimitiveType(Kind.String), makePrimitiveType(Kind.Int32));
    let m = new MapLeaf(ms, tr, [{key: 'a', value: 4}, {key:'k', value:8}]);

    let kv = [];
    await m.forEach((v, k) => { kv.push(k, v); });
    assert.deepEqual(['a', 4, 'k', 8], kv);
  });
});

suite('CompoundMap', () => {
  function build(): Array<CompoundMap> {
    let ms = new MemoryStore();
    let tr = makeCompoundType(Kind.Map, makePrimitiveType(Kind.String), makePrimitiveType(Kind.Bool));
    let l1 = new MapLeaf(ms, tr, [{key: 'a', value: false}, {key:'b', value:false}]);
    let r1 = writeValue(l1, tr, ms);
    let l2 = new MapLeaf(ms, tr, [{key: 'e', value: true}, {key:'f', value:true}]);
    let r2 = writeValue(l2, tr, ms);
    let l3 = new MapLeaf(ms, tr, [{key: 'h', value: false}, {key:'i', value:true}]);
    let r3 = writeValue(l3, tr, ms);
    let l4 = new MapLeaf(ms, tr, [{key: 'm', value: true}, {key:'n', value:false}]);
    let r4 = writeValue(l4, tr, ms);

    let m1 = new CompoundMap(ms, tr, [new MetaTuple(r1, 'b'), new MetaTuple(r2, 'f')]);
    let rm1 = writeValue(m1, tr, ms);
    let m2 = new CompoundMap(ms, tr, [new MetaTuple(r3, 'i'), new MetaTuple(r4, 'n')]);
    let rm2 = writeValue(m2, tr, ms);

    let c = new CompoundMap(ms, tr, [new MetaTuple(rm1, 'f'), new MetaTuple(rm2, 'n')]);
    return [c, m1, m2];
  }

  test('get', async () => {
    let [c] = build();

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

  test('first/has', async () => {
    let [c, m1, m2] = build();


    assert.deepEqual(['a', false], await c.first());
    assert.deepEqual(['a', false], await m1.first());
    assert.deepEqual(['h', false], await m2.first());

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

    let kv = [];
    await c.forEach((v, k) => { kv.push(k, v); });
    assert.deepEqual(['a', false, 'b', false, 'e', true, 'f', true, 'h', false, 'i', true, 'm', true, 'n', false], kv);
  });
});
