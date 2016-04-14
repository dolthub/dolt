// @flow

import {suite, test} from 'mocha';
import {assert} from 'chai';
import {Package, registerPackage} from './package.js';
import {
  boolType,
  Field,
  float64Type,
  makeListType,
  makeMapType,
  makeSetType,
  makeStructType,
  makeType,
  stringType,
  uint8Type,
} from './type.js';
import {defToNoms} from './defs.js';
import {newList} from './list.js';
import {newStruct} from './struct.js';
import {newSet} from './set.js';
import {newMap} from './map.js';
import {emptyRef} from './ref.js';
import {ValueBase} from './value.js';
import {invariant} from './assert.js';

suite('defs', () => {
  test('string', async () => {
    const s1 = await defToNoms('abc', stringType);
    assert.equal(s1, 'abc');
  });

  test('number', async () => {
    const v = await defToNoms(123, uint8Type);
    assert.equal(v, 123);
  });

  test('boolean', async () => {
    const v = await defToNoms(true, boolType);
    assert.equal(v, true);
  });

  test('list', async () => {
    const listOfUint8Type = makeListType(uint8Type);
    const l1 = await newList([0, 1, 2, 3], listOfUint8Type);
    const l2 = await defToNoms([0, 1, 2, 3], listOfUint8Type);
    invariant(l2 instanceof ValueBase);
    assert.isTrue(l1.equals(l2));
  });

  test('set', async () => {
    const setOfFloat64Type = makeSetType(float64Type);
    const s1 = await newSet([0, 1, 2, 3], setOfFloat64Type);
    const s2 = await defToNoms([0, 1, 2, 3], setOfFloat64Type);
    invariant(s2 instanceof ValueBase);
    assert.isTrue(s1.equals(s2));
  });

  test('map', async () => {
    const mapOfFloat64ToStringType = makeMapType(float64Type, stringType);
    const m1 = await newMap([0, 'zero', 1, 'one'], mapOfFloat64ToStringType);
    const m2 = await defToNoms([0, 'zero', 1, 'one'], mapOfFloat64ToStringType);
    invariant(m2 instanceof ValueBase);
    assert.isTrue(m1.equals(m2));
  });

  test('struct', async () => {
    let typeDef;
    const pkg = new Package([
      typeDef = makeStructType('Struct', [
        new Field('b', boolType, false),
        new Field('s', stringType, false),
      ], []),
    ], []);
    registerPackage(pkg);
    const type = makeType(pkg.ref, 0);

    const s1 = newStruct(type, typeDef, {
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
    let typeDef;
    const listOfUint8Type = makeListType(uint8Type);
    const pkg = new Package([
      typeDef = makeStructType('StructWithList', [
        new Field('l', listOfUint8Type, false),
      ], []),
    ], []);
    registerPackage(pkg);
    const type = makeType(pkg.ref, 0);

    const s1 = newStruct(type, typeDef, {
      l: await newList([0, 1, 2, 3], listOfUint8Type),
    });

    const s2 = await defToNoms({
      l: [0, 1, 2, 3],
    }, type);

    invariant(s2 instanceof ValueBase);
    assert.isTrue(s1.equals(s2));
  });

  test('list of struct', async () => {
    let typeDef;
    const pkg = new Package([
      typeDef = makeStructType('Struct', [
        new Field('i', uint8Type, false),
      ], []),
    ], []);
    registerPackage(pkg);
    const structType = makeType(pkg.ref, 0);
    const listType = makeListType(structType);

    const l1 = await newList([
      newStruct(structType, typeDef, {i: 1}),
      newStruct(structType, typeDef, {i: 2}),
    ], listType);

    const l2 = await defToNoms([{i: 1}, {i: 2}], listType);

    invariant(l2 instanceof ValueBase);
    assert.isTrue(l1.equals(l2));
  });

  test('recursive struct', async () => {
    const pkg = new Package([
      makeStructType('Struct', [
        new Field('children', makeListType(makeType(emptyRef, 0)), false),
      ], []),
    ], []);
    registerPackage(pkg);
    const type = makeType(pkg.ref, 0);
    const typeDef = makeStructType('Struct', [
      new Field('children', makeListType(makeType(pkg.ref, 0)), false),
    ], []);

    const listType = makeListType(type);

    const a = await newList([], listType);
    const b = await newList([], listType);
    const x = newStruct(type, typeDef, {
      children: a,
    });
    const y = newStruct(type, typeDef, {
      children: b,
    });
    const c = await newList([x, y], listType);

    const t1 = newStruct(type, typeDef, {
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
  });
});
