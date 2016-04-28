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
} from './type.js';
import {suite, test} from 'mocha';
import DataStore from './data-store.js';

suite('Type', () => {
  test('types', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);

    const mapType = makeMapType(stringType, numberType);
    const setType = makeSetType(stringType);
    const mahType = makeStructType('MahStruct', [
      new Field('Field1', stringType, false),
      new Field('Field2', boolType, true),
    ], []);
    const otherType = makeStructType('MahOtherStruct', [], [
      new Field('StructField', mahType, false),
      new Field('StringField', stringType, false),
    ]);

    const otherRef = ds.writeValue(otherType).targetRef;
    const mapRef = ds.writeValue(mapType).targetRef;
    const setRef = ds.writeValue(setType).targetRef;
    const mahRef = ds.writeValue(mahType).targetRef;

    assert.isTrue(otherType.equals(await ds.readValue(otherRef)));
    assert.isTrue(mapType.equals(await ds.readValue(mapRef)));
    assert.isTrue(setType.equals(await ds.readValue(setRef)));
    assert.isTrue(mahType.equals(await ds.readValue(mahRef)));
  });

  test('type Type', () => {
    assert.isTrue(boolType.type.equals(typeType));
  });
});
