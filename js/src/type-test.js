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
    const db = new Database(makeTestingBatchStore());

    const mapType = makeMapType(stringType, numberType);
    const setType = makeSetType(stringType);
    const mahType = makeStructType('MahStruct', {
      'Field1': stringType,
      'Field2': boolType,
    });

    const mapRef = db.writeValue(mapType).targetHash;
    const setRef = db.writeValue(setType).targetHash;
    const mahRef = db.writeValue(mahType).targetHash;

    assert.isTrue(equals(mapType, await db.readValue(mapRef)));
    assert.isTrue(equals(setType, await db.readValue(setRef)));
    assert.isTrue(equals(mahType, await db.readValue(mahRef)));
    await db.close();
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

  test('verify struct field name', () => {
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
    assertInvalid('💩');

    function assertValid(n: string) {
      makeStructType('S', {[n]: stringType});
    }
    assertValid('a');
    assertValid('A');
    assertValid('a0');
    assertValid('a_');
    assertValid('a0_');
  });

  test('verify struct name', () => {
    function assertInvalid(n: string) {
      assert.throw(() => {
        makeStructType(n, {});
      });
    }
    assertInvalid(' ');
    assertInvalid(' a');
    assertInvalid('a ');
    assertInvalid('0');
    assertInvalid('_');
    assertInvalid('0a');
    assertInvalid('_a');
    assertInvalid('💩');

    function assertValid(n: string) {
      makeStructType(n, {});
    }
    assertValid('');
    assertValid('a');
    assertValid('A');
    assertValid('a0');
    assertValid('a_');
    assertValid('a0_');
  });
});
