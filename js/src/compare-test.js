// @flow

import {suite, test} from 'mocha';
import {assert} from 'chai';
import {getCompareFunction, compare, equals} from './compare.js';
import {
  boolType,
  makeListType,
  numberType,
  stringType,
} from './type.js';
import {newList} from './list.js';

suite('compare', () => {
  suite('getCompareFunction', () => {
    test('int8', () => {
      const compare = getCompareFunction(numberType);
      assert.equal(compare(1, 1), 0);
      assert.equal(compare(1, 3), -2);
      assert.equal(compare(4, 2), 2);
    });

    test('string', () => {
      const compare = getCompareFunction(stringType);
      assert.equal(compare('a', 'a'), 0);
      assert.equal(compare('a', 'b'), -1);
      assert.equal(compare('c', 'a'), 1);
    });

    test('bool', () => {
      const compare = getCompareFunction(boolType);
      assert.equal(compare(true, true), 0);
      assert.equal(compare(true, false), -1);
      assert.equal(compare(false, true), 1);
    });

    test('list', async () => {
      const listOfNumberType = makeListType(numberType);
      const compare = getCompareFunction(listOfNumberType);
      const listA = await newList([0, 1, 2, 3], listOfNumberType);
      const listB = await newList([0, 1, 2, 3], listOfNumberType);
      const listC = await newList([4, 5, 6, 7], listOfNumberType);
      assert.equal(compare(listA, listA), 0);
      assert.equal(compare(listA, listB), 0);
      assert.equal(compare(listA, listC), -1);
      assert.equal(compare(listC, listA), 1);
    });
  });

  suite('compare', () => {
    test('int8', () => {
      assert.equal(compare(1, 1), 0);
      assert.equal(compare(1, 3), -1);
      assert.equal(compare(4, 2), 1);
    });

    test('string', () => {
      assert.equal(compare('a', 'a'), 0);
      assert.equal(compare('a', 'b'), -1);
      assert.equal(compare('c', 'a'), 1);
    });

    test('bool', () => {
      assert.equal(compare(true, true), 0);
      assert.equal(compare(true, false), -1);
      assert.equal(compare(false, true), 1);
    });

    test('list', async () => {
      const listOfNumberType = makeListType(numberType);
      const listA = await newList([0, 1, 2, 3], listOfNumberType);
      const listB = await newList([0, 1, 2, 3], listOfNumberType);
      const listC = await newList([4, 5, 6, 7], listOfNumberType);
      assert.equal(compare(listA, listA), 0);
      assert.equal(compare(listA, listB), 0);
      assert.equal(compare(listA, listC), -1);
      assert.equal(compare(listC, listA), 1);
    });
  });

  suite('equal', () => {
    test('int8', () => {
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
      const listOfNumberType = makeListType(numberType);
      const listA = await newList([0, 1, 2, 3], listOfNumberType);
      const listB = await newList([0, 1, 2, 3], listOfNumberType);
      const listC = await newList([4, 5, 6, 7], listOfNumberType);
      assert.isTrue(equals(listA, listA));
      assert.isTrue(equals(listA, listB));
      assert.isFalse(equals(listA, listC));
      assert.isFalse(equals(listC, listA));
    });
  });
});
