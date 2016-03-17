// @flow

import {suite, test} from 'mocha';
import {assert} from 'chai';
import Random from './pseudo_random.js';

suite('pseudo random', () => {
  test('sequence', () => {
    const r = new Random(42);
    const a = [];
    for (let i = 0; i < 16; i++) {
      a.push(r.nextUint8());
    }
    assert.deepEqual(a, [
      129,
      236,
      91,
      254,
      69,
      224,
      191,
      18,
      73,
      20,
      99,
      102,
      141,
      136,
      71,
      250,
    ]);
  });

  test('deterministic', () => {
    const r = new Random(123);
    const r2 = new Random(123);
    for (let i = 0; i < 1e3; i++) {
      assert.equal(r.nextUint8(), r2.nextUint8());
    }
  });
});
