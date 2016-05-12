// @flow

import {makeTestingBatchStore} from './batch-store-adaptor.js';
import {assert} from 'chai';
import {
  boolType,
  makeMapType,
  makeSetType,
  makeStructType,
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
});
