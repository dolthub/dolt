// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';

import {
  StructWithUnions,
  __unionOfBOfFloat64AndCOfString,
  __unionOfEOfFloat64AndFOfString,
 } from './gen/struct_with_unions.noms.js';

suite('struct_optional.noms', () => {
  test('constructor', async () => {
    // TODO: This needs to be cleaner.
    const swu = new StructWithUnions({
      a: new __unionOfBOfFloat64AndCOfString({b: 1}),
      d: new __unionOfEOfFloat64AndFOfString({f:'hi'}),
    });
    assert.equal(swu.a.b, 1);
    assert.equal(swu.d.f, 'hi');

    const swu2 = swu.setA(swu.a.setC('bye'));
    const swu3 = new StructWithUnions({
      a: new __unionOfBOfFloat64AndCOfString({c: 'bye'}),
      d: new __unionOfEOfFloat64AndFOfString({f:'hi'}),
    });
    assert.isTrue(swu2.equals(swu3));
  });
});
