// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';
import eq from './eq.js';
import {Ref} from 'noms';

suite('eq', () => {
  test('different', () => {
    const r0 = Ref.parse('sha1-0000000000000000000000000000000000000000');
    const r1 = Ref.parse('sha1-0000000000000000000000000000000000000001');

    const different = [
      null,
      undefined,
      true,
      false,
      1,
      2,
      '1',
      {},
      {a: 3},
      {a: 4},
      {a: 3, b: 5},
      [],
      [6],
      [7],
      [6, 7],
      new Set(),
      new Set([8]),
      new Set([9]),
      new Set([8, 9]),
      new Map(),
      new Map([[10, 11]]),
      new Map([[10, 12]]),
      new Map([[10, 11], [13, 14]]),
      r0,
      r1,
    ];
    for (let i = 0; i < different.length; i++) {
      for (let j = 0; j < different.length; j++) {
        assert.equal(i === j, eq(different[i], different[j]), `${different[i]} == ${different[j]}`);
      }
    }
  });

  test('same', () => {
    const r1 = Ref.parse('sha1-0000000000000000000000000000000000000000');
    const r2 = Ref.parse('sha1-0000000000000000000000000000000000000000');
    const same = [
      [new Set([1, 2]), new Set([2, 1])],
      [new Map([[1, 2], [3, 4]]), new Map([[3, 4], [1, 2]])],
      [{a: 1, b: 2}, {b: 2, a: 1}],
      [new Set([{a: 1}]), new Set([{a: 1}])],
      [new Map([[{a: 1}, {b: 2}]]), new Map([[{a: 1}, {b: 2}]])],
      [new Set([r1]), new Set([r2])],
      [new Map([[r1, 42]]), new Map([[r2, 42]])],
    ];
    for (const vs of same) {
      for (let i = 0; i < vs.length; i++) {
        for (let j = 0; j < vs.length; j++) {
          assert.equal(true, eq(vs[i], vs[i]), `${vs[i]} == ${vs[j]}`);
        }
      }
    }
  });
});
