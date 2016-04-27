// @flow

import {assert} from 'chai';  //eslint-disable-line
import {suite, test} from 'mocha';

import {newBlob} from '@attic/noms';
import {StructPrimitives} from './gen/struct_primitives.noms.js';

suite('struct-primitives.noms', () => {
  test('constructor', async () => {
    const s: StructPrimitives = new StructPrimitives({  //eslint-disable-line
      number: 9,
      bool: true,
      string: 'hi',
      blob: await newBlob(new Uint8Array([0, 1, 2, 3])),
      value: 123,
    });

    let s2;
    assert.equal(s.number, 9);
    s2 = s.setNumber(99);
    assert.equal(s2.number, 99);

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
