// @flow

import {suite, test} from 'mocha';
import {assert} from 'chai';
import {
  boolType,
  Field,
  makeListType,
  makeMapType,
  makeSetType,
  makeStructType,
  stringType,
  numberType,
  valueType,
  StructDesc,
} from './type.js';
import {defToNoms} from './defs.js';
import {newList} from './list.js';
import {newStruct} from './struct.js';
import {newSet} from './set.js';
import {newMap} from './map.js';
import {ValueBase} from './value.js';
import {invariant} from './assert.js';

suite('defs', () => {
  test('string', async () => {
    const s1 = await defToNoms('abc', stringType);
    assert.equal(s1, 'abc');
  });

  test('number', async () => {
    const v = await defToNoms(123, numberType);
    assert.equal(v, 123);
  });

  test('boolean', async () => {
    const v = await defToNoms(true, boolType);
    assert.equal(v, true);
  });

  test('list', async () => {
    const listOfNumberType = makeListType(numberType);
    const l1 = await newList([0, 1, 2, 3], listOfNumberType);
    const l2 = await defToNoms([0, 1, 2, 3], listOfNumberType);
    invariant(l2 instanceof ValueBase);
    assert.isTrue(l1.equals(l2));

    const l3 = await defToNoms(l1, listOfNumberType);
    invariant(l3 instanceof ValueBase);
    assert.isTrue(l1.equals(l3));

    let ex;
    try {
      await defToNoms(l1, makeListType(stringType));
    } catch (e) {
      ex = e;
    }
    assert.ok(ex);
  });

  test('set', async () => {
    const setOfNumberType = makeSetType(numberType);
    const s1 = await newSet([0, 1, 2, 3], setOfNumberType);
    const s2 = await defToNoms([0, 1, 2, 3], setOfNumberType);
    invariant(s2 instanceof ValueBase);
    assert.isTrue(s1.equals(s2));

    let ex;
    try {
      await defToNoms(s1, makeSetType(stringType));
    } catch (e) {
      ex = e;
    }
    assert.ok(ex);
  });

  test('map', async () => {
    const mapOfNumberToStringType = makeMapType(numberType, stringType);
    const m1 = await newMap([0, 'zero', 1, 'one'], mapOfNumberToStringType);
    const m2 = await defToNoms([0, 'zero', 1, 'one'], mapOfNumberToStringType);
    invariant(m2 instanceof ValueBase);
    assert.isTrue(m1.equals(m2));

    let ex;
    try {
      await defToNoms(m1, makeMapType(stringType, numberType));
    } catch (e) {
      ex = e;
    }
    assert.ok(ex);
  });

  test('struct', async () => {
    const type = makeStructType('Struct', [
      new Field('b', boolType, false),
      new Field('s', stringType, false),
    ], []);

    const s1 = newStruct(type, {
      b: true,
      s: 'hi',
    });

    const s2 = await defToNoms({
      b: true,
      s: 'hi',
    }, type);
    assert.isTrue(s1.equals(s2));
  });

  test('struct with list', async () => {
    const listOfNumberType = makeListType(numberType);
    const type = makeStructType('StructWithList', [
      new Field('l', listOfNumberType, false),
    ], []);

    const s1 = newStruct(type, {
      l: await newList([0, 1, 2, 3], listOfNumberType),
    });

    const s2 = await defToNoms({
      l: [0, 1, 2, 3],
    }, type);

    invariant(s2 instanceof ValueBase);
    assert.isTrue(s1.equals(s2));
  });

  test('list of struct', async () => {
    const structType = makeStructType('Struct', [
      new Field('i', numberType, false),
    ], []);
    const listType = makeListType(structType);

    const l1 = await newList([
      newStruct(structType, {i: 1}),
      newStruct(structType, {i: 2}),
    ], listType);

    const l2 = await defToNoms([{i: 1}, {i: 2}], listType);
    invariant(l2 instanceof ValueBase);
    assert.isTrue(l1.equals(l2));

    const l3 = await defToNoms([newStruct(structType, {i: 1}), {i: 2}], listType);
    invariant(l3 instanceof ValueBase);
    assert.isTrue(l1.equals(l3));
  });

  test('recursive struct', async () => {
    const type = makeStructType('Struct', [
      new Field('children', valueType /* placeholder */, false),
    ], []);
    const listType = makeListType(type);
    invariant(type.desc instanceof StructDesc);
    type.desc.fields[0].t = listType;

    const a = await newList([], listType);
    const b = await newList([], listType);
    const x = newStruct(type, {
      children: a,
    });
    const y = newStruct(type, {
      children: b,
    });
    const c = await newList([x, y], listType);

    const t1 = newStruct(type, {
      children: c,
    });

    const t2 = await defToNoms({
      children: [
        {children: []},
        {children: []},
      ],
    }, type);

    invariant(t2 instanceof ValueBase);
    assert.isTrue(t1.equals(t2));

    const t3 = await defToNoms({
      children: [
        {children: []},
        {children: await newList([], listType)},
      ],
    }, type);

    invariant(t3 instanceof ValueBase);
    assert.isTrue(t1.equals(t3));
  });
});
