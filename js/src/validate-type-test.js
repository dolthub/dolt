// @flow

import {assert} from 'chai';
import {newBlob} from './blob.js';
import {newList} from './list.js';
import {newMap} from './map.js';
import {newSet} from './set.js';
import {newStruct} from './struct.js';
import {suite, test} from 'mocha';
import validateType from './validate-type.js';
import type {Type} from './type.js';
import {
  blobType,
  boolType,
  Field,
  listOfValueType,
  makeListType,
  makeMapType,
  makeSetType,
  makeStructType,
  mapOfValueType,
  numberType,
  setOfValueType,
  stringType,
  typeType,
  valueType,
} from './type.js';

suite('validate type', () => {

  function assertInvalid(t: Type, v) {
    assert.throws(() => { validateType(t, v); });
  }

  const allTypes = [
    boolType,
    numberType,
    stringType,
    blobType,
    typeType,
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
    validateType(numberType, 42);
    validateType(stringType, 'abc');

    assertInvalid(boolType, 1);
    assertInvalid(boolType, 'abc');
    assertInvalid(numberType, true);
    assertInvalid(stringType, 42);
  });

  test('value', async () => {
    validateType(valueType, true);
    validateType(valueType, 1);
    validateType(valueType, 'abc');
    const listOfNumberType = makeListType(numberType);
    const l = await newList([0, 1, 2, 3], listOfNumberType);
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
    const listOfNumberType = makeListType(numberType);
    const l = await newList([0, 1, 2, 3], listOfNumberType);
    validateType(listOfNumberType, l);
    assertAll(listOfNumberType, l);

    validateType(listOfValueType, l);
  });

  test('map', async () => {
    const mapOfNumberToStringType = makeMapType(numberType, stringType);
    const m = await newMap([0, 'a', 2, 'b'], mapOfNumberToStringType);
    validateType(mapOfNumberToStringType, m);
    assertAll(mapOfNumberToStringType, m);

    validateType(mapOfValueType, m);
  });

  test('set', async () => {
    const setOfNumberType = makeSetType(numberType);
    const s = await newSet([0, 1, 2, 3], setOfNumberType);
    validateType(setOfNumberType, s);
    assertAll(setOfNumberType, s);

    validateType(setOfValueType, s);
  });

  test('type', () => {
    const t = makeSetType(numberType);
    validateType(typeType, t);
    assertAll(typeType, t);

    validateType(valueType, t);
  });

  test('struct', async () => {
    const type = makeStructType('Struct', [
      new Field('x', boolType, false),
    ]);

    const v = newStruct(type, {x: true});
    validateType(type, v);
    assertAll(type, v);

    validateType(valueType, v);
  });
});
