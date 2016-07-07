// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {assert} from 'chai';
import {suite, test} from 'mocha';

import {TypeWriter} from './encode-human-readable.js';
import {
  blobType,
  boolType,
  numberType,
  makeRefType,
  makeListType,
  makeMapType,
  makeSetType,
  makeStructType,
  makeUnionType,
  stringType,
  valueType,
  Type,
} from './type.js';

suite('Encode human readable types', () => {
  function assertWriteType(expected: string, t: Type) {
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

    assertWriteType('String | Number', makeUnionType([numberType, stringType]));
    assertWriteType('Bool', makeUnionType([boolType]));
    assertWriteType('', makeUnionType([]));
    assertWriteType('List<String | Number>', makeListType(makeUnionType([numberType, stringType])));
    assertWriteType('List<>', makeListType(makeUnionType([])));
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
        valueType,  // placeholder
        valueType,  // placeholder
        valueType,  // placeholder
      ]
    );
    const d = makeStructType('D',
      ['e', 'f'],
      [
        valueType,  // placeholder
        a,
      ]
    );
    a.desc.setField('b', a);
    a.desc.setField('d', d);
    d.desc.setField('e', d);
    d.desc.setField('f', a);
    a.desc.setField('c', makeListType(a));

    assertWriteType(`struct A {
  b: Cycle<0>,
  c: List<Cycle<0>>,
  d: struct D {
    e: Cycle<0>,
    f: Cycle<1>,
  },
}`, a);

    assertWriteType(`struct D {
  e: Cycle<0>,
  f: struct A {
    b: Cycle<0>,
    c: List<Cycle<0>>,
    d: Cycle<1>,
  },
}`, d);
  });
});
