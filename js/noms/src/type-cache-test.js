// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {assert} from 'chai';
import {suite, test} from 'mocha';
import {Kind} from './noms-kind.js';
import {equals} from './compare.js';
import {
  blobType,
  boolType,
  makeCycleType,
  makeListType,
  makeRefType,
  makeSetType,
  makeStructType,
  makeUnionType,
  numberType,
  stringType,
  typeType,
  valueType,
} from './type.js';

suite('TypeCache', () => {
  test('list', () => {
    const lbt = makeListType(boolType);
    const lbt2 = makeListType(boolType);
    assert.strictEqual(lbt, lbt2);

    const lst = makeListType(stringType);
    const lnt = makeListType(numberType);
    assert.notEqual(lst, lnt);

    const lst2 = makeListType(stringType);
    assert.strictEqual(lst, lst2);

    const lnt2 = makeListType(numberType);
    assert.strictEqual(lnt, lnt2);

    const lbt3 = makeListType(boolType);
    assert.strictEqual(lbt, lbt3);
  });

  test('set', () => {
    const lbt = makeSetType(boolType);
    const lbt2 = makeSetType(boolType);
    assert.strictEqual(lbt, lbt2);

    const lst = makeSetType(stringType);
    const lnt = makeSetType(numberType);
    assert.notEqual(lst, lnt);

    const lst2 = makeSetType(stringType);
    assert.strictEqual(lst, lst2);

    const lnt2 = makeSetType(numberType);
    assert.strictEqual(lnt, lnt2);

    const lbt3 = makeSetType(boolType);
    assert.strictEqual(lbt, lbt3);
  });

  test('ref', () => {
    const lbt = makeRefType(boolType);
    const lbt2 = makeRefType(boolType);
    assert.strictEqual(lbt, lbt2);

    const lst = makeRefType(stringType);
    const lnt = makeRefType(numberType);
    assert.notEqual(lst, lnt);

    const lst2 = makeRefType(stringType);
    assert.strictEqual(lst, lst2);

    const lnt2 = makeRefType(numberType);
    assert.strictEqual(lnt, lnt2);

    const lbt3 = makeRefType(boolType);
    assert.strictEqual(lbt, lbt3);
  });

  test('struct', () => {
    const st = makeStructType('Foo',
      ['bar', 'foo'],
      [stringType, numberType]
    );
    const st2 = makeStructType('Foo',
      ['bar', 'foo'],
      [stringType, numberType]
    );

    assert.strictEqual(st, st2);
  });

  test('union', () => {
    let ut = makeUnionType([numberType]);
    let ut2 = makeUnionType([numberType]);
    assert.strictEqual(ut, ut2);
    assert.strictEqual(ut2, numberType);

    ut = makeUnionType([numberType, stringType]);
    ut2 = makeUnionType([stringType, numberType]);
    assert.strictEqual(ut, ut2);

    ut = makeUnionType([stringType, boolType, numberType]);
    ut2 = makeUnionType([numberType, stringType, boolType]);
    assert.strictEqual(ut, ut2);
  });

  test('Cyclic Struct', () => {
    const st = makeStructType('Foo',
      ['foo'],
      [
        makeRefType(makeCycleType(0)),
      ]);
    assert.isFalse(st.hasUnresolvedCycle([]));
    assert.strictEqual(st, st.desc.fields[0].type.desc.elemTypes[0]);

    const st2 = makeStructType('Foo',
      ['foo'],
      [
        makeRefType(makeCycleType(0)),
      ]);
    assert.isFalse(st2.hasUnresolvedCycle([]));
    assert.strictEqual(st, st2);
  });

  test('Cyclic Unions', () => {
    const ut = makeUnionType([makeCycleType(0), numberType, stringType, boolType, blobType,
                              valueType, typeType]);
    const st = makeStructType('Foo', ['foo'], [ut]);

    assert.strictEqual(ut.desc.elemTypes[0].kind, Kind.Cycle);
    assert.strictEqual(st, st.desc.fields[0].type.desc.elemTypes[1]);
    assert.isFalse(equals(ut, st.desc.fields[0].type));

    // Note that the union in this second case has a different provided ordering of it's element
    // types.
    const ut2 = makeUnionType([numberType, stringType, boolType, blobType, valueType, typeType,
                               makeCycleType(0)]);
    const st2 = makeStructType('Foo', ['foo'], [ut]);

    assert.strictEqual(ut2.desc.elemTypes[0].kind, Kind.Cycle);
    assert.strictEqual(st2, st2.desc.fields[0].type.desc.elemTypes[1]);
    assert.isFalse(equals(ut2, st2.desc.fields[0].type));

    assert.strictEqual(ut, ut2);
    assert.strictEqual(st, st2);
  });

  test('Invalid Cycles and Unions', () => {
    assert.throws(() => {
      makeStructType('A', ['a'], [makeStructType('A', ['a'], [makeCycleType(1)])]);
    });
  });

  test('Invalid Crazy Cycles and Unions', () => {
    /*
     * struct A {
     *   a: Union {
     *     Cycle <0> |
     *     Struct A {
     *       a: Union {
     *         Cycle <0> |
     *         Cycle <1>
     *       }
     *     }
     *   }
     * }
     */
    assert.throws(() => {
      makeStructType('A', ['a'], [makeUnionType(
        [makeCycleType(0), makeStructType('A', ['a'], [makeCycleType(0), makeCycleType(1)])]
      )]);
    });
  });
});
