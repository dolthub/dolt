// @flow

import {suite, test} from 'mocha';
import {assert} from 'chai';
import {equals, compare} from './compare.js';
import {
  boolType,
} from './type.js';
import {newBlob} from './blob.js';
import {newList} from './list.js';
import {newMap} from './map.js';
import {newSet} from './set.js';
import {newStruct} from './struct.js';
import Database from './database.js';
import {makeTestingBatchStore} from './batch-store-adaptor.js';

suite('compare.js', () => {
  suite('compare', () => {
    test('number', () => {
      assert.equal(compare(1, 1), 0);
      assert.isBelow(compare(1, 3), 0);
      assert.isAbove(compare(4, 2), 0);
    });

    test('string', () => {
      assert.equal(compare('a', 'a'), 0);
      assert.isBelow(compare('a', 'b'), 0);
      assert.isAbove(compare('c', 'a'), 0);
    });

    test('bool', () => {
      assert.equal(compare(true, true), 0);
      assert.isBelow(compare(false, true), 0);
      assert.isAbove(compare(true, false), 0);

    });

    test('list', async () => {
      const listA = await newList([0, 1, 2, 3]);
      const listB = await newList([0, 1, 2, 3]);
      const listC = await newList([4, 5, 6, 7]);
      assert.equal(compare(listA, listA), 0);
      assert.equal(compare(listA, listB), 0);
      // These two are ordered by hash
      assert.isAbove(compare(listA, listC), 0);
      assert.isBelow(compare(listC, listA), 0);
    });

    test('union', async () => {
      const listA = await newList([0, 'b', 2, 'd']);
      const listB = await newList([0, 'b', 2, 'd']);
      const listC = await newList([4, 5, 'x', 7]);
      assert.equal(compare(listA, listA), 0);
      assert.equal(compare(listA, listB), 0);
      assert.isBelow(compare(listA, listC), 0);
      assert.isAbove(compare(listC, listA), 0);
    });

    test('total ordering', async () => {
      const db = new Database(makeTestingBatchStore());

      // values in increasing order. Some of these are compared by ref so changing the serialization
      // might change the ordering.
      const values = [
        false, true,
        -10, 0, 10,
        'a', 'b', 'c',

        // The order of these are done by the hash.
        db.writeValue(10),
        await newSet([0, 1, 2, 3]),
        await newMap([[0, 1], [2, 3]]),
        boolType,
        await newBlob(new Uint8Array([0, 1, 2, 3])),
        await newList([0, 1, 2, 3]),
        newStruct('', {x: 1, s: 'a'}),

        // Value - values cannot be value
        // Parent - values cannot be parent
        // Union - values cannot be unions
      ];

      for (let i = 0; i < values.length; i++) {
        for (let j = 0; j < values.length; j++) {
          if (i === j) {
            assert.equal(compare(values[i], values[j]), 0);
          } else if (i < j) {
            assert.isBelow(compare(values[i], values[j]), 0);
          } else {
            assert.isAbove(compare(values[i], values[j]), 0);
          }
        }
      }
      await db.close();
    });
  });

  suite('equal', () => {
    test('number', () => {
      assert.isTrue(equals(1, 1));
      assert.isFalse(equals(1, 3));
      assert.isFalse(equals(4, 2));
    });

    test('string', () => {
      assert.isTrue(equals('a', 'a'));
      assert.isFalse(equals('a', 'b'));
      assert.isFalse(equals('c', 'a'));
    });

    test('bool', () => {
      assert.isTrue(equals(true, true));
      assert.isFalse(equals(true, false));
      assert.isFalse(equals(false, true));
    });

    test('list', async () => {
      const listA = await newList([0, 1, 2, 3]);
      const listB = await newList([0, 1, 2, 3]);
      const listC = await newList([4, 5, 6, 7]);
      assert.isTrue(equals(listA, listA));
      assert.isTrue(equals(listA, listB));
      assert.isFalse(equals(listA, listC));
      assert.isFalse(equals(listC, listA));
    });
  });
});
