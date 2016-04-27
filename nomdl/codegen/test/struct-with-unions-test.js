// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';

import {
  StructWithUnions,
  __unionOfBOfNumberAndCOfString,
  __unionOfEOfNumberAndFOfString,
 } from './gen/struct_with_unions.noms.js';

suite('struct_optional.noms', () => {
  test('constructor', async () => {
    // TODO: This needs to be cleaner.
    const swu = new StructWithUnions({
      a: new __unionOfBOfNumberAndCOfString({b: 1}),
      d: new __unionOfEOfNumberAndFOfString({f:'hi'}),
    });
    assert.equal(swu.a.b, 1);
    assert.equal(swu.d.f, 'hi');

    const swu2 = swu.setA(swu.a.setC('bye'));
    const swu3 = new StructWithUnions({
      a: new __unionOfBOfNumberAndCOfString({c: 'bye'}),
      d: new __unionOfEOfNumberAndFOfString({f:'hi'}),
    });
    assert.isTrue(swu2.equals(swu3));
  });
});
