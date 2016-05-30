// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

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

    assertWriteType('Number | String', makeUnionType([numberType, stringType]));
    assertWriteType('Bool', makeUnionType([boolType]));
    assertWriteType('', makeUnionType([]));
    assertWriteType('List<Number | String>', makeListType(makeUnionType([numberType, stringType])));
    assertWriteType('List<>', makeListType(makeUnionType([])));
  });

  test('struct', () => {
    const type = makeStructType('S1', {
      'x': numberType,
      'y': stringType,
    });
    assertWriteType('struct S1 {\n  x: Number\n  y: String\n}', type);
  });


  test('list of struct', () => {
    const type = makeStructType('S3', {
      'x': numberType,
    });
    assertWriteType('List<struct S3 {\n  x: Number\n}>', makeListType(type));
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

    const a = makeStructType('A', {
      'b': valueType,  // placeholder
      'c': valueType,  // placeholder
      'd': valueType,  // placeholder
    });
    const d = makeStructType('D', {
      'e': valueType,  // placeholder
      'f': a,
    });
    a.desc.fields['b'] = a;
    a.desc.fields['d'] = d;
    d.desc.fields['e'] = d;
    d.desc.fields['f'] = a;
    a.desc.fields['c'] = makeListType(a);

    assertWriteType(`struct A {
  b: Parent<0>
  c: List<Parent<0>>
  d: struct D {
    e: Parent<0>
    f: Parent<1>
  }
}`, a);

    assertWriteType(`struct D {
  e: Parent<0>
  f: struct A {
    b: Parent<0>
    c: List<Parent<0>>
    d: Parent<1>
  }
}`, d);
  });
});
