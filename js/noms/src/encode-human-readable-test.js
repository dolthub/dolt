// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {assert} from 'chai';
import {suite, test} from 'mocha';

import {invariant, notNull} from './assert.js';
import {TypeWriter} from './encode-human-readable.js';
import {
  blobType,
  boolType,
  numberType,
  makeCycleType,
  makeRefType,
  makeListType,
  makeMapType,
  makeSetType,
  makeStructType,
  makeUnionType,
  stringType,
  StructDesc,
  Type,
} from './type.js';

suite('Encode human readable types', () => {
  function assertWriteType(expected: string, t: Type<any>) {
    let actual = '';
    const w = {
      write(s: string) {
        actual += s;
      },
    };
    const tw = new TypeWriter(w);
    tw.writeType(t);
    assert.equal(actual, expected);
  }

  test('primitives', () => {
    assertWriteType('Bool', boolType);
    assertWriteType('Blob', blobType);
    assertWriteType('String', stringType);
    assertWriteType('Number', numberType);
  });

  test('compound', () => {
    assertWriteType('List<Number>', makeListType(numberType));
    assertWriteType('Set<Number>', makeSetType(numberType));
    assertWriteType('Ref<Number>', makeRefType(numberType));
    assertWriteType('Map<Number, String>', makeMapType(numberType, stringType));

    assertWriteType('Number | String', makeUnionType([numberType, stringType]));
    assertWriteType('Bool', makeUnionType([boolType]));
    assertWriteType('', makeUnionType([]));
    assertWriteType('List<Number | String>', makeListType(makeUnionType([numberType, stringType])));
    assertWriteType('List<>', makeListType(makeUnionType([])));
    assertWriteType('Set<>', makeSetType(makeUnionType([])));
    assertWriteType('Map<>', makeMapType(makeUnionType([]), makeUnionType([])));
  });

  test('struct', () => {
    const type = makeStructType('S1',
      ['x', 'y'],
      [
        numberType,
        stringType,
      ]
    );
    assertWriteType('struct S1 {\n  x: Number,\n  y: String,\n}', type);
  });


  test('list of struct', () => {
    const type = makeStructType('S3', ['x'], [numberType]);
    assertWriteType('List<struct S3 {\n  x: Number,\n}>', makeListType(type));
  });

  test('recursive struct', () => {
    // struct A {
    //   b: A
    //   c: List<A>
    //   d: struct D {
    //     e: D
    //     f: A
    //   }
    // }

    const a = makeStructType('A',
      ['b', 'c', 'd'],
      [
        makeCycleType(0),
        makeListType(makeCycleType(0)),
        makeStructType('D',
          ['e', 'f'],
          [
            makeCycleType(0),
            makeCycleType(1),
          ]
        ),
      ]
    );

    assertWriteType(`struct A {
  b: Cycle<0>,
  c: List<Cycle<0>>,
  d: struct D {
    e: Cycle<0>,
    f: Cycle<1>,
  },
}`, a);

    invariant(a.desc instanceof StructDesc);
    const d = notNull(a.desc.getField('d'));

    assertWriteType(`struct D {
  e: Cycle<0>,
  f: struct A {
    b: Cycle<0>,
    c: List<Cycle<0>>,
    d: Cycle<1>,
  },
}`, d);
  });

  test('recursive unresolved struct', () => {
    // struct A {
    //   a: A
    //   b: Cycle<1>
    // }

    const a = makeStructType('A',
      ['a', 'b'],
      [
        makeCycleType(0),
        makeCycleType(1),
      ]
    );

    assertWriteType(`struct A {
  a: Cycle<0>,
  b: Cycle<1>,
}`, a);
  });
});
