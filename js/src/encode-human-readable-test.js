// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';

import {TypeWriter} from './encode-human-readable.js';
import {
  blobType,
  boolType,
  Field,
  numberType,
  makeRefType,
  makeListType,
  makeMapType,
  makeSetType,
  makeStructType,
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
  });

  test('struct', () => {
    const type = makeStructType('S1', [
      new Field('x', numberType),
      new Field('y', stringType),
    ]);
    assertWriteType('struct S1 {\n  x: Number\n  y: String\n}', type);
  });


  test('list of struct', () => {
    const type = makeStructType('S3', [
      new Field('x', numberType),
    ]);
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

    const a = makeStructType('A', [
      new Field('b', valueType /* placeholder */),
      new Field('c', valueType /* placeholder */),
      new Field('d', valueType /* placeholder */),
    ]);
    const d = makeStructType('D', [
      new Field('e', valueType /* placeholder */),
      new Field('f', a),
    ]);
    const aDesc = a.desc;
    const dDesc = d.desc;
    aDesc.fields[0].type = a;
    aDesc.fields[2].type = d;
    dDesc.fields[0].type = d;
    dDesc.fields[1].type = a;
    aDesc.fields[1].type = makeListType(a);


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
