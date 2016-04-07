// @flow

import {assert} from 'chai';  //eslint-disable-line
import {suite, test} from 'mocha';

import {newBlob} from '@attic/noms';
import {StructPrimitives} from './gen/struct_primitives.noms.js';

suite('struct-primitives.noms', () => {
  test('constructor', async () => {
    const s: StructPrimitives = new StructPrimitives({  //eslint-disable-line
      uint64: 1,
      uint32: 2,
      uint16: 3,
      uint8: 4,
      int64: 5,
      int32: 6,
      int16: 7,
      int8: 8,
      float64: 9,
      float32: 10,
      bool: true,
      string: 'hi',
      blob: await newBlob(new Uint8Array([0, 1, 2, 3])),
      value: 123,
    });

    let s2;
    assert.equal(s.uint64, 1);
    s2 = s.setUint64(11);
    assert.equal(s2.uint64, 11);

    assert.equal(s.uint32, 2);
    s2 = s.setUint32(22);
    assert.equal(s2.uint32, 22);

    assert.equal(s.uint16, 3);
    s2 = s.setUint16(33);
    assert.equal(s2.uint16, 33);

    assert.equal(s.uint8, 4);
    s2 = s.setUint8(44);
    assert.equal(s2.uint8, 44);

    assert.equal(s.int64, 5);
    s2 = s.setInt64(55);
    assert.equal(s2.int64, 55);

    assert.equal(s.int32, 6);
    s2 = s.setInt32(66);
    assert.equal(s2.int32, 66);

    assert.equal(s.int16, 7);
    s2 = s.setInt16(77);
    assert.equal(s2.int16, 77);

    assert.equal(s.int8, 8);
    s2 = s.setInt8(88);
    assert.equal(s2.int8, 88);

    assert.equal(s.float64, 9);
    s2 = s.setFloat64(99);
    assert.equal(s2.float64, 99);

    assert.equal(s.float32, 10);
    s2 = s.setFloat32(1010);
    assert.equal(s2.float32, 1010);

    assert.equal(s.bool, true);
    s2 = s.setBool(false);
    assert.equal(s2.bool, false);

    assert.equal(s.string, 'hi');
    s2 = s.setString('bye');
    assert.equal(s2.string, 'bye');

    assert.isTrue(s.blob.equals(await newBlob(new Uint8Array([0, 1, 2, 3]))));
    s2 = s.setBlob(await newBlob(new Uint8Array([4, 5, 6, 7])));
    assert.isTrue(s2.blob.equals(await newBlob(new Uint8Array([4, 5, 6, 7]))));

    assert.equal(s.value, 123);
    s2 = s.setValue('x');
    assert.equal(s2.value, 'x');
  });
});
