// @flow

import {assert} from 'chai';
import {newBlob} from './blob.js';
import {newList} from './list.js';
import {newMap} from './map.js';
import {newSet} from './set.js';
import {newStruct} from './struct.js';
import {suite, test} from 'mocha';
import assertSubtype from './assert-type.js';
import type {Type} from './type.js';
import {
  blobType,
  boolType,
  listOfValueType,
  makeListType,
  makeMapType,
  makeRefType,
  makeSetType,
  makeStructType,
  makeUnionType,
  mapOfValueType,
  numberType,
  setOfValueType,
  stringType,
  typeType,
  valueType,
} from './type.js';
import {equals} from './compare.js';
import RefValue from './ref-value.js';

suite('validate type', () => {

  function assertInvalid(t: Type, v) {
    assert.throws(() => { assertSubtype(t, v); });
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
      if (at === valueType || equals(t, at)) {
        assertSubtype(at, v);
      } else {
        assertInvalid(at, v);
      }
    }
  }

  test('primitives', () => {
    assertSubtype(boolType, true);
    assertSubtype(boolType, false);
    assertSubtype(numberType, 42);
    assertSubtype(stringType, 'abc');

    assertInvalid(boolType, 1);
    assertInvalid(boolType, 'abc');
    assertInvalid(numberType, true);
    assertInvalid(stringType, 42);
  });

  test('value', async () => {
    assertSubtype(valueType, true);
    assertSubtype(valueType, 1);
    assertSubtype(valueType, 'abc');
    const l = await newList([0, 1, 2, 3]);
    assertSubtype(valueType, l);
  });

  test('blob', async () => {
    const b = await newBlob(new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7]));
    assertAll(blobType, b);
  });

  test('list', async () => {
    const listOfNumberType = makeListType(numberType);
    const l = await newList([0, 1, 2, 3]);
    assertSubtype(listOfNumberType, l);
    assertAll(listOfNumberType, l);

    assertSubtype(listOfValueType, l);
  });

  test('map', async () => {
    const mapOfNumberToStringType = makeMapType(numberType, stringType);
    const m = await newMap([0, 'a', 2, 'b']);
    assertSubtype(mapOfNumberToStringType, m);
    assertAll(mapOfNumberToStringType, m);

    assertSubtype(mapOfValueType, m);
  });

  test('set', async () => {
    const setOfNumberType = makeSetType(numberType);
    const s = await newSet([0, 1, 2, 3]);
    assertSubtype(setOfNumberType, s);
    assertAll(setOfNumberType, s);

    assertSubtype(setOfValueType, s);
  });

  test('type', () => {
    const t = makeSetType(numberType);
    assertSubtype(typeType, t);
    assertAll(typeType, t);

    assertSubtype(valueType, t);
  });

  test('struct', async () => {
    const type = makeStructType('Struct', {
      'x': boolType,
    });

    const v = newStruct('Struct', {x: true});
    assertSubtype(type, v);
    assertAll(type, v);

    assertSubtype(valueType, v);
  });

  test('union', async () => {
    assertSubtype(makeUnionType([numberType]), 42);
    assertSubtype(makeUnionType([numberType, stringType]), 42);
    assertSubtype(makeUnionType([numberType, stringType]), 'hi');
    assertSubtype(makeUnionType([numberType, stringType, boolType]), 555);
    assertSubtype(makeUnionType([numberType, stringType, boolType]), 'hi');
    assertSubtype(makeUnionType([numberType, stringType, boolType]), true);

    const lt = makeListType(makeUnionType([numberType, stringType]));
    assertSubtype(lt, await newList([1, 'hi', 2, 'bye']));

    const st = makeSetType(stringType);
    assertSubtype(makeUnionType([st, numberType]), 42);
    assertSubtype(makeUnionType([st, numberType]), await newSet(['a', 'b']));

    assertInvalid(makeUnionType([]), 42);
    assertInvalid(makeUnionType([stringType]), 42);
    assertInvalid(makeUnionType([stringType, boolType]), 42);
    assertInvalid(makeUnionType([st, stringType]), 42);
    assertInvalid(makeUnionType([st, numberType]), await newSet([1, 2]));
  });

  test('empty list union', async () => {
    const lt = makeListType(makeUnionType([]));
    assertSubtype(lt, await newList([]));
  });

  test('empty list', async () => {
    const lt = makeListType(numberType);
    assertSubtype(lt, await newList([]));

    // List<> not a subtype of List<Number>
    assertInvalid(makeListType(makeUnionType([])), await newList([1]));
  });

  test('empty set', async () => {
    const st = makeSetType(numberType);
    assertSubtype(st, await newSet([]));

    // Set<> not a subtype of Set<Number>
    assertInvalid(makeSetType(makeUnionType([])), await newSet([1]));
  });

  test('empty map', async () => {
    const mt = makeMapType(numberType, stringType);
    assertSubtype(mt, await newMap([]));

    // Map<> not a subtype of Map<Number, Number>
    assertInvalid(makeMapType(makeUnionType([]), makeUnionType([])), await newMap([1, 2]));
  });

  test('struct subtype by name', () => {
    const namedT = makeStructType('Name', {x: numberType});
    const anonT = makeStructType('', {x: numberType});
    const namedV = newStruct('Name', {x: 42});
    const name2V = newStruct('foo', {x: 42});
    const anonV = newStruct('', {x: 42});

    assertSubtype(namedT, namedV);
    assertInvalid(namedT, name2V);
    assertInvalid(namedT, anonV);

    assertSubtype(anonT, namedV);
    assertSubtype(anonT, name2V);
    assertSubtype(anonT, anonV);
  });

  test('struct subtype extra fields', () => {
    const at = makeStructType('', {});
    const bt = makeStructType('', {x: numberType});
    const ct = makeStructType('', {x: numberType, s: stringType});
    const av = newStruct('', {});
    const bv = newStruct('', {x: 1});
    const cv = newStruct('', {x: 2, s: 'hi'});

    assertSubtype(at, av);
    assertInvalid(bt, av);
    assertInvalid(ct, av);

    assertSubtype(at, bv);
    assertSubtype(bt, bv);
    assertInvalid(ct, bv);

    assertSubtype(at, cv);
    assertSubtype(bt, cv);
    assertSubtype(ct, cv);
  });

  test('struct subtype', async () => {
    const c1 = newStruct('Commit', {
      value: 1,
      parents: await newSet([]),
    });
    const t1 = makeStructType('Commit', {
      value: numberType,
      parents: makeSetType(makeUnionType([])),
    });
    assertSubtype(t1, c1);

    const t11 = makeStructType('Commit', {
      value: numberType,
      parents: makeSetType(makeRefType(numberType /* placeholder */)),
    });
    t11.desc.fields['parents'].desc.elemTypes[0].desc.elemTypes[0] = t11;
    assertSubtype(t11, c1);

    const c2 = newStruct('Commit', {
      value: 2,
      parents: await newSet([new RefValue(c1)]),
    });
    assertSubtype(t11, c2);

    // struct { v: V, p: Set<> } <!
    // struct { v: V, p: Set<Ref<...>> }
    assertInvalid(t1, c2);
  });
});
