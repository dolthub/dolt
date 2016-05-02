// @flow

import {makeTestingBatchStore} from './batch-store-adaptor.js';
import {assert} from 'chai';
import {
  boolType,
  Field,
  makeMapType,
  makeSetType,
  makeStructType,
  numberType,
  stringType,
  typeType,
  getTypeOfValue,
} from './type.js';
import {suite, test} from 'mocha';
import DataStore from './data-store.js';

suite('Type', () => {
  test('types', async () => {
    const ds = new DataStore(makeTestingBatchStore());

    const mapType = makeMapType(stringType, numberType);
    const setType = makeSetType(stringType);
    const mahType = makeStructType('MahStruct', [
      new Field('Field1', stringType),
      new Field('Field2', boolType),
    ]);

    const mapRef = ds.writeValue(mapType).targetRef;
    const setRef = ds.writeValue(setType).targetRef;
    const mahRef = ds.writeValue(mahType).targetRef;

    assert.isTrue(mapType.equals(await ds.readValue(mapRef)));
    assert.isTrue(setType.equals(await ds.readValue(setRef)));
    assert.isTrue(mahType.equals(await ds.readValue(mahRef)));
  });

  test('type Type', () => {
    assert.isTrue(boolType.type.equals(typeType));
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
