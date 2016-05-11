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

  test('verify field name', () => {
    function assertInvalid(n: string) {
      assert.throw(() => {
        makeStructType('S', {[n]: stringType});
      });
    }
    assertInvalid('');
    assertInvalid(' ');
    assertInvalid(' a');
    assertInvalid('a ');
    assertInvalid('0');
    assertInvalid('_');
    assertInvalid('0a');
    assertInvalid('_a');

    function assertValid(n: string) {
      makeStructType('S', {[n]: stringType});
    }
    assertValid('a');
    assertValid('A');
    assertValid('a0');
    assertValid('a_');
    assertValid('a0_');
  });
});
