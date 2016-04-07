// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';

import {StructWithUnionField} from './gen/struct_with_union_field.noms.js';

suite('struct_optional.noms', () => {
  test('constructor', async () => {
    const swuf = new StructWithUnionField({a: 1, b: 2});
    assert.equal(swuf.a, 1);
    assert.equal(swuf.b, 2);
    assert.isUndefined(swuf.c);
    assert.isUndefined(swuf.d);
    assert.isUndefined(swuf.e);
    assert.isUndefined(swuf.f);

    const swuf2 = swuf.setC('hi');
    assert.equal(swuf2.a, 1);
    assert.isUndefined(swuf2.b);
    assert.equal(swuf2.c, 'hi');
    assert.isUndefined(swuf2.d);
    assert.isUndefined(swuf2.e);
    assert.isUndefined(swuf2.f);

    assert.throws(() => {
      swuf.setC(undefined);
    });
  });
});
