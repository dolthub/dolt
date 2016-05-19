// @flow

import {makeTestingBatchStore} from './batch-store-adaptor.js';
import Struct, {
  newStruct,
  newStructWithType,
  StructMirror,
  structDiff,
  createStructClass,
} from './struct.js';
import {assert} from 'chai';
import {
  boolType,
  makeListType,
  makeStructType,
  numberType,
  stringType,
  valueType,
} from './type.js';
import {suite, test} from 'mocha';
import Database from './database.js';
import {equals} from './compare.js';
import {newList} from './list.js';
import {newMap} from './map.js';
import {newSet} from './set.js';

suite('Struct', () => {
  test('equals', () => {
    const data1 = {x: true, o: 'hi'};
    const s1 = newStruct('S1', data1);
    const s2 = newStruct('S1', data1);

    assert.isTrue(equals(s1, s2));
  });

  test('chunks', () => {
    const ds = new Database(makeTestingBatchStore());

    const b = true;
    const r = ds.writeValue(b);
    const s1 = newStruct('S1', {r: r});
    assert.strictEqual(1, s1.chunks.length);
    assert.isTrue(equals(r, s1.chunks[0]));
  });

  test('new', () => {
    const s1 = newStruct('S2', {b: true, o: 'hi'});
    assert.strictEqual(s1.b, true);
    assert.strictEqual(s1.o, 'hi');

    const s2 = newStruct('S2', {b: false, o: 'hi'});
    assert.strictEqual(s2.b, false);
    assert.strictEqual(s2.o, 'hi');

    const s3 = newStruct('S2', {b: true, o: 'hi'});
    assert.isTrue(equals(s1, s3));
  });

  test('struct set', () => {
    const s1 = newStruct('S3', {b: true, o: 'hi'});
    const s2 = s1.setB(false);

    // TODO: assert throws on set wrong type
    assert.throws(() => {
      s1.setX(1);
    });

    const s3 = s2.setB(true);
    assert.isTrue(equals(s1, s3));

    const m = new StructMirror(s1);
    const s4 = m.set('b', false);
    assert.isTrue(equals(s2, s4));

    const s5 = s3.setO('bye');
    const s6 = new StructMirror(s3).set('o', 'bye');
    assert.isTrue(equals(s5, s6));
  });

  test('createStructClass', () => {
    const typeA = makeStructType('A', {
      'b': numberType,
      'c': stringType,
    });
    const A = createStructClass(typeA);
    const a = new A({b: 1, c: 'hi'});
    assert.instanceOf(a, Struct);
    assert.instanceOf(a, A);
    assert.equal(a.b, 1);
    assert.equal(a.c, 'hi');
  });

  test('type validation', () => {
    const type = makeStructType('S1', {
      'x': boolType,
      'o': stringType,
    });

    assert.throws(() => {
      newStructWithType(type, {x: 1, o: 'hi'});
    });
    assert.throws(() => {
      newStructWithType(type, {o: 1});
    });

    newStructWithType(type, {x: true, o: 'hi'});
  });

  test('type validation cyclic', async () => {
    // struct S {
    //   b: Bool
    //   l: List<S>
    // }
    const type = makeStructType('S', {
      'b': boolType,
      'l': valueType, // placeholder
    });
    const listType = makeListType(type);
    type.desc.fields['l'] = listType;

    const emptyList = await newList([], listType);
    newStructWithType(type, {b: true, l: emptyList});
    newStructWithType(type, {b: true, l: await newList([
      newStructWithType(type, {b: false, l: emptyList}),
    ], listType)});

    assert.throws(() => {
      newStructWithType(type, {b: 1});
    });
    assert.throws(() => {
      newStructWithType(type, {b: true, o: 1});
    });
  });

  function assertDiff(expect: [string], s1: Struct, s2: Struct) {
    const actual = structDiff(s1, s2);
    assert.deepEqual(expect, actual);
  }

  test('diff', async () => {
    const s1 = newStruct('', {a: true, b: 'hi', c: 4});

    assertDiff([], s1, newStruct('', {a: true, b: 'hi', c: 4}));
    assertDiff(['a', 'b'], s1, newStruct('', {a: false, b: 'bye', c: 4}));
    assertDiff(['b', 'c'], s1, newStruct('', {a: true, b: 'bye', c: 5}));
    assertDiff(['a', 'c'], s1, newStruct('', {a: false, b: 'hi', c: 10}));

    const s2 = newStruct('', {
      a: await newList([0, 1]),
      b: await newMap([['foo', false], ['bar', false]]),
      c: await newSet([0, 1, 'foo']),
    });

    assertDiff([], s2, newStruct('', {
      a: await newList([0, 1]),
      b: await newMap([['foo', false], ['bar', false]]),
      c: await newSet([0, 1, 'foo']),
    }));

    assertDiff(['a', 'b'], s2, newStruct('', {
      a: await newList([1, 1]),
      b: await newMap([['foo', false], ['bar', true]]),
      c: await newSet([0, 1, 'foo']),
    }));

    assertDiff(['a', 'c'], s2, newStruct('', {
      a: await newList([0]),
      b: await newMap([['foo', false], ['bar', false]]),
      c: await newSet([0, 1, 'bar']),
    }));

    assertDiff(['b', 'c'], s2, newStruct('', {
      a: await newList([0, 1]),
      b: await newMap([['boo', false], ['bar', true]]),
      c: await newSet([0, 1, 'bar']),
    }));
  });
});
