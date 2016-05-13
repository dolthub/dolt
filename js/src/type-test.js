// @flow

import {makeTestingBatchStore} from './batch-store-adaptor.js';
import {assert} from 'chai';
import {
  boolType,
  makeMapType,
  makeSetType,
  makeStructType,
  makeUnionType,
  numberType,
  stringType,
  typeType,
  getTypeOfValue,
} from './type.js';
import {suite, test} from 'mocha';
import Database from './database.js';
import {equals} from './compare.js';

suite('Type', () => {
  test('types', async () => {
    const ds = new Database(makeTestingBatchStore());

    const mapType = makeMapType(stringType, numberType);
    const setType = makeSetType(stringType);
    const mahType = makeStructType('MahStruct', {
      'Field1': stringType,
      'Field2': boolType,
    });

    const mapRef = ds.writeValue(mapType).targetRef;
    const setRef = ds.writeValue(setType).targetRef;
    const mahRef = ds.writeValue(mahType).targetRef;

    assert.isTrue(equals(mapType, await ds.readValue(mapRef)));
    assert.isTrue(equals(setType, await ds.readValue(setRef)));
    assert.isTrue(equals(mahType, await ds.readValue(mahRef)));
  });

  test('type Type', () => {
    assert.isTrue(equals(boolType.type, typeType));
  });

  test('getTypeOfValue', () => {
    assert.equal(boolType, getTypeOfValue(true));
    assert.equal(boolType, getTypeOfValue(false));
    assert.equal(numberType, getTypeOfValue(42));
    assert.equal(numberType, getTypeOfValue(0));
    assert.equal(stringType, getTypeOfValue('abc'));
    assert.equal(typeType, getTypeOfValue(stringType));
  });

  test('flatten union types', () => {
    assert.equal(makeUnionType([boolType]), boolType);
    assert.deepEqual(makeUnionType([]), makeUnionType([]));
    assert.deepEqual(makeUnionType([boolType, makeUnionType([stringType])]),
                     makeUnionType([boolType, stringType]));
    assert.deepEqual(makeUnionType([boolType, makeUnionType([stringType, numberType])]),
                     makeUnionType([boolType, stringType, numberType]));
    assert.equal(makeUnionType([boolType, boolType]), boolType);
    assert.equal(makeUnionType([boolType, makeUnionType([])]), boolType);
    assert.equal(makeUnionType([makeUnionType([]), boolType]), boolType);
    assert.isTrue(equals(makeUnionType([makeUnionType([]), makeUnionType([])]), makeUnionType([])));
    assert.deepEqual(makeUnionType([boolType, numberType]), makeUnionType([boolType, numberType]));
    assert.deepEqual(makeUnionType([numberType, boolType]), makeUnionType([boolType, numberType]));
    assert.deepEqual(makeUnionType([boolType, numberType, boolType]),
                     makeUnionType([boolType, numberType]));
    assert.deepEqual(makeUnionType([makeUnionType([boolType, numberType]), numberType, boolType]),
                     makeUnionType([boolType, numberType]));
  });
});
