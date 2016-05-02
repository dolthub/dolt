// @flow

import MemoryStore from './memory-store.js';
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
import Database from './database.js';

suite('Type', () => {
  test('types', async () => {
    const ms = new MemoryStore();
    const db = new Database(ms);

    const mapType = makeMapType(stringType, numberType);
    const setType = makeSetType(stringType);
    const mahType = makeStructType('MahStruct', [
      new Field('Field1', stringType),
      new Field('Field2', boolType),
    ]);

    const mapRef = db.writeValue(mapType).targetRef;
    const setRef = db.writeValue(setType).targetRef;
    const mahRef = db.writeValue(mahType).targetRef;

    assert.isTrue(mapType.equals(await db.readValue(mapRef)));
    assert.isTrue(setType.equals(await db.readValue(setRef)));
    assert.isTrue(mahType.equals(await db.readValue(mahRef)));
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
