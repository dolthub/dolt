// @flow

import {assert} from 'chai';
import {newBlob} from './blob.js';
import {newList} from './list.js';
import {newMap} from './map.js';
import {newSet} from './set.js';
import {newStruct} from './struct.js';
import {suite, test} from 'mocha';
import {Package, registerPackage} from './package.js';
import validateType from './validate-type.js';
import type {Type} from './type.js';
import {
  blobType,
  boolType,
  Field,
  float32Type,
  float64Type,
  int16Type,
  int32Type,
  int64Type,
  int8Type,
  listOfValueType,
  makeListType,
  makeMapType,
  makeSetType,
  makeStructType,
  makeType,
  mapOfValueType,
  packageType,
  setOfValueType,
  stringType,
  typeType,
  uint16Type,
  uint32Type,
  uint64Type,
  uint8Type,
  valueType,
} from './type.js';

suite('validate type', () => {

  function assertInvalid(t: Type, v) {
    assert.throws(() => { validateType(t, v); });
  }

  const allTypes = [
    boolType,
    uint8Type,
    uint16Type,
    uint32Type,
    uint64Type,
    int8Type,
    int16Type,
    int32Type,
    int64Type,
    float32Type,
    float64Type,
    stringType,
    blobType,
    typeType,
    packageType,
    valueType,
  ];

  function assertAll(t: Type, v) {
    for (const at of allTypes) {
      if (at === valueType || t.equals(at)) {
        validateType(at, v);
      } else {
        assertInvalid(at, v);
      }
    }
  }

  test('primitives', () => {
    validateType(boolType, true);
    validateType(boolType, false);
    validateType(uint8Type, 42);
    validateType(uint16Type, 42);
    validateType(uint32Type, 42);
    validateType(uint64Type, 42);
    validateType(int8Type, 42);
    validateType(int16Type, 42);
    validateType(int32Type, 42);
    validateType(int64Type, 42);
    validateType(float32Type, 42);
    validateType(float64Type, 42);
    validateType(stringType, 'abc');

    assertInvalid(boolType, 1);
    assertInvalid(boolType, 'abc');
    assertInvalid(uint8Type, true);
    assertInvalid(uint16Type, true);
    assertInvalid(uint32Type, true);
    assertInvalid(uint64Type, true);
    assertInvalid(int8Type, true);
    assertInvalid(int16Type, true);
    assertInvalid(int32Type, true);
    assertInvalid(int64Type, true);
    assertInvalid(float32Type, true);
    assertInvalid(float64Type, true);
    assertInvalid(stringType, 42);
  });

  test('value', async () => {
    validateType(valueType, true);
    validateType(valueType, 1);
    validateType(valueType, 'abc');
    const listOfUint8Type = makeListType(uint8Type);
    const l = await newList([0, 1, 2, 3], listOfUint8Type);
    validateType(valueType, l);

    assertInvalid(valueType, null);
    assertInvalid(valueType, undefined);
    assertInvalid(valueType, {});
  });

  test('blob', async () => {
    const b = await newBlob(new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7]));
    assertAll(blobType, b);
  });

  test('list', async () => {
    const listOfUint8Type = makeListType(uint8Type);
    const l = await newList([0, 1, 2, 3], listOfUint8Type);
    validateType(listOfUint8Type, l);
    assertAll(listOfUint8Type, l);

    validateType(listOfValueType, l);
  });

  test('map', async () => {
    const mapOfUint8ToStringType = makeMapType(uint8Type, stringType);
    const m = await newMap([0, 'a', 2, 'b'], mapOfUint8ToStringType);
    validateType(mapOfUint8ToStringType, m);
    assertAll(mapOfUint8ToStringType, m);

    validateType(mapOfValueType, m);
  });

  test('set', async () => {
    const setOfUint8Type = makeSetType(uint8Type);
    const s = await newSet([0, 1, 2, 3], setOfUint8Type);
    validateType(setOfUint8Type, s);
    assertAll(setOfUint8Type, s);

    validateType(setOfValueType, s);
  });

  test('type', () => {
    const t = makeSetType(uint8Type);
    validateType(typeType, t);
    assertAll(typeType, t);

    validateType(valueType, t);
  });

  test('package', async () => {
    const pkg = new Package([], []);
    validateType(packageType, pkg);
    assertAll(packageType, pkg);

    validateType(valueType, pkg);
  });

  test('struct', async () => {
    const typeDef = makeStructType('Struct', [
      new Field('x', boolType, false),
    ], []);
    const pkg = new Package([typeDef], []);
    registerPackage(pkg);
    const pkgRef = pkg.ref;
    const type = makeType(pkgRef, 0);

    const v = newStruct(type, typeDef, {x: true});
    validateType(type, v);
    assertAll(type, v);

    validateType(valueType, v);
  });
});
