// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {assert} from 'chai';
import Blob from './blob.js';
import List from './list.js';
import Map from './map.js';
import Set from './set.js';
import {newStruct} from './struct.js';
import {suite, test} from 'mocha';
import assertSubtype, {isSubtype} from './assert-type.js';
import type {Type} from './type.js';
import {
  blobType,
  boolType,
  makeCycleType,
  makeListType,
  makeMapType,
  makeRefType,
  makeSetType,
  makeStructType,
  makeUnionType,
  numberType,
  stringType,
  typeType,
  valueType,
} from './type.js';
import {equals} from './compare.js';
import Ref from './ref.js';

suite('validate type', () => {

  function assertInvalid(t: Type<any>, v) {
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

  function assertAll(t: Type<any>, v) {
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

  test('value', () => {
    assertSubtype(valueType, true);
    assertSubtype(valueType, 1);
    assertSubtype(valueType, 'abc');
    const l = new List([0, 1, 2, 3]);
    assertSubtype(valueType, l);
  });

  test('blob', () => {
    const b = new Blob(new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7]));
    assertAll(blobType, b);
  });

  test('list', () => {
    const listOfNumberType = makeListType(numberType);
    const l = new List([0, 1, 2, 3]);
    assertSubtype(listOfNumberType, l);
    assertAll(listOfNumberType, l);

    assertSubtype(makeListType(valueType), l);
  });

  test('map', () => {
    const mapOfNumberToStringType = makeMapType(numberType, stringType);
    const m = new Map([[0, 'a'], [2, 'b']]);
    assertSubtype(mapOfNumberToStringType, m);
    assertAll(mapOfNumberToStringType, m);

    assertSubtype(makeMapType(valueType, valueType), m);
  });

  test('set', () => {
    const setOfNumberType = makeSetType(numberType);
    const s = new Set([0, 1, 2, 3]);
    assertSubtype(setOfNumberType, s);
    assertAll(setOfNumberType, s);

    assertSubtype(makeSetType(valueType), s);
  });

  test('type', () => {
    const t = makeSetType(numberType);
    assertSubtype(typeType, t);
    assertAll(typeType, t);

    assertSubtype(valueType, t);
  });

  test('struct', () => {
    const type = makeStructType('Struct', ['x'], [boolType]);

    const v = newStruct('Struct', {x: true});
    assertSubtype(type, v);
    assertAll(type, v);

    assertSubtype(valueType, v);
  });

  test('union', () => {
    assertSubtype(makeUnionType([numberType]), 42);
    assertSubtype(makeUnionType([numberType, stringType]), 42);
    assertSubtype(makeUnionType([numberType, stringType]), 'hi');
    assertSubtype(makeUnionType([numberType, stringType, boolType]), 555);
    assertSubtype(makeUnionType([numberType, stringType, boolType]), 'hi');
    assertSubtype(makeUnionType([numberType, stringType, boolType]), true);

    const lt = makeListType(makeUnionType([numberType, stringType]));
    assertSubtype(lt, new List([1, 'hi', 2, 'bye']));

    const st = makeSetType(stringType);
    assertSubtype(makeUnionType([st, numberType]), 42);
    assertSubtype(makeUnionType([st, numberType]), new Set(['a', 'b']));

    assertInvalid(makeUnionType([]), 42);
    assertInvalid(makeUnionType([stringType]), 42);
    assertInvalid(makeUnionType([stringType, boolType]), 42);
    assertInvalid(makeUnionType([st, stringType]), 42);
    assertInvalid(makeUnionType([st, numberType]), new Set([1, 2]));
  });

  test('empty list union', () => {
    const lt = makeListType(makeUnionType([]));
    assertSubtype(lt, new List());
  });

  test('empty list', () => {
    const lt = makeListType(numberType);
    assertSubtype(lt, new List());

    // List<> not a subtype of List<Number>
    assertInvalid(makeListType(makeUnionType([])), new List([1]));
  });

  test('empty set', () => {
    const st = makeSetType(numberType);
    assertSubtype(st, new Set());

    // Set<> not a subtype of Set<Number>
    assertInvalid(makeSetType(makeUnionType([])), new Set([1]));
  });

  test('empty map', () => {
    const mt = makeMapType(numberType, stringType);
    assertSubtype(mt, new Map());

    // Map<> not a subtype of Map<Number, Number>
    assertInvalid(makeMapType(makeUnionType([]), makeUnionType([])), new Map([[1, 2]]));
  });

  test('struct subtype by name', () => {
    const namedT = makeStructType('Name', ['x'], [numberType]);
    const anonT = makeStructType('', ['x'], [numberType]);
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
    const at = makeStructType('', [], []);
    const bt = makeStructType('', ['x'], [numberType]);
    const ct = makeStructType('', ['s', 'x'], [stringType, numberType]);
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

  test('struct subtype', () => {
    const c1 = newStruct('Commit', {
      value: 1,
      parents: new Set(),
    });
    const t1 = makeStructType('Commit',
      ['parents', 'value'],
      [
        makeSetType(makeUnionType([])),
        numberType,
      ]
    );
    assertSubtype(t1, c1);

    const t11 = makeStructType('Commit',
      ['parents', 'value'],
      [
        makeSetType(makeRefType(t1)),
        numberType,
      ]
    );
    assertSubtype(t11, c1);

    const c2 = newStruct('Commit', {
      value: 2,
      parents: new Set([new Ref(c1)]),
    });
    assertSubtype(t11, c2);

    // struct { v: V, p: Set<> } <!
    // struct { v: V, p: Set<Ref<...>> }
    assertInvalid(t1, c2);
  });

  test('CycleUnion', () => {
    // struct {
    //   x: Cycle<0>,
    //   y: Number,
    // }
    const t1 = makeStructType('', ['x', 'y'], [
      makeCycleType(0),
      numberType,
    ]);
    // struct {
    //   x: Cycle<0>,
    //   y: Number | String,
    // }
    const t2 = makeStructType('', ['x', 'y'], [
      makeCycleType(0),
      makeUnionType([numberType, stringType]),
    ]);

    assert.isTrue(isSubtype(t2, t1, []));
    assert.isFalse(isSubtype(t1, t2, []));

    // struct {
    //   x: Cycle<0> | Number,
    //   y: Number | String,
    // }
    const t3 = makeStructType('', ['x', 'y'], [
      makeUnionType([makeCycleType(0), numberType]),
      makeUnionType([numberType, stringType]),
    ]);

    assert.isTrue(isSubtype(t3, t1, []));
    assert.isFalse(isSubtype(t1, t3, []));

    assert.isTrue(isSubtype(t3, t2, []));
    assert.isFalse(isSubtype(t2, t3, []));

    // struct {
    //   x: Cycle<0> | Number,
    //   y: Number,
    // }
    const t4 = makeStructType('', ['x', 'y'], [
      makeUnionType([makeCycleType(0), numberType]),
      numberType,
    ]);

    assert.isTrue(isSubtype(t4, t1, []));
    assert.isFalse(isSubtype(t1, t4, []));

    assert.isFalse(isSubtype(t4, t2, []));
    assert.isFalse(isSubtype(t2, t4, []));

    assert.isTrue(isSubtype(t3, t4, []));
    assert.isFalse(isSubtype(t4, t3, []));

    // struct B {
    //   b: struct C {
    //     c: Cycle<1>,
    //   },
    // }

    // struct C {
    //   c: struct B {
    //     b: Cycle<1>,
    //   },
    // }
    const tb = makeStructType('', ['b'], [
      makeStructType('', ['c'], [
        makeCycleType(1),
      ]),
    ]);
    const tc = makeStructType('', ['c'], [
      makeStructType('', ['b'], [
        makeCycleType(1),
      ]),
    ]);

    assert.isFalse(isSubtype(tb, tc, []));
    assert.isFalse(isSubtype(tc, tb, []));
  });

});
