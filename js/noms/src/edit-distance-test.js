// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {suite, test} from 'mocha';
import type {Splice} from './edit-distance.js';
import {assert} from 'chai';
import {DEFAULT_MAX_SPLICE_MATRIX_SIZE, calcSplices} from './edit-distance.js';

suite('Edit Distance', () => {

  function assertDiff<T>(last: Array<T>, current: Array<T>, expect: Array<Splice>) {
    assert.deepEqual(expect, calcSplices(last.length, current.length,
      DEFAULT_MAX_SPLICE_MATRIX_SIZE,
      (i, j) => last[i] === current[j]));
  }

  test('append', () => {
    assertDiff(
      [0, 1, 2],
      [0, 1, 2, 3, 4, 5],
      [[3, 0, 3, 3]]);
  });

  test('prepend', () => {
    assertDiff(
      [3, 4, 5, 6],
      [0, 1, 2, 3, 4, 5, 6],
      [[0, 0, 3, 0]]);
  });

  test('chop from end', () => {
    assertDiff(
      [0, 1, 2, 3, 4, 5],
      [0, 1, 2],
      [[3, 3, 0, 0]]);
  });

  test('chop from start', () => {
    assertDiff(
      [0, 1, 2, 3, 4, 5],
      [3, 4, 5],
      [[0, 3, 0, 0]]);
  });

  test('chop from middle', () => {
    assertDiff(
      [0, 1, 2, 3, 4, 5],
      [0, 5],
      [[1, 4, 0, 0]]);
  });

  test('a', () => {
    assertDiff(
      [0, 1, 2, 3, 4, 5, 6, 7, 8],
      [0, 1, 2, 4, 5, 6, 8],
      [[3, 1, 0, 0], [7, 1, 0, 0]]);
  });

  test('remove a bunch', () => {
    assertDiff(
      [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      [1, 2, 4, 5, 7, 8, 10],
      [[0, 1, 0, 0], [3, 1, 0, 0], [6, 1, 0, 0], [9, 1, 0, 0]]);
  });

  test('add a bunch', () => {
    assertDiff(
      [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      [0, 'a', 1, 2, 3, 'b', 'c', 'd', 4, 5, 6, 7, 'e', 8, 9, 'f', 10, 'g'],
      [[1, 0, 1, 1], [4, 0, 3, 5], [8, 0, 1, 12], [10, 0, 1, 15], [11, 0, 1, 17]]);
  });

  test('update a bunch', () => {
    assertDiff(
      [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      ['a', 1, 2, 'b', 'c', 'd', 6, 7, 'e', 9, 10],
      [[0, 1, 1, 0], [3, 3, 3, 3], [8, 1, 1, 8]]);
  });

  test('left-overlap', () => {
    assertDiff(
      [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      [0, 1, 2, 3, 'a', 'b', 8, 9, 10],
      [[4, 4, 2, 4]]);
  });

  test('right-overlap', () => {
    assertDiff(
      [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      [0, 1, 2, 3, 4, 5, 'a', 'b', 10],
      [[6, 4, 2, 6]]);
  });

  test('within', () => {
    assertDiff(
      [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      [0, 1, 2, 3, 'a', 'b', 10],
      [[4, 6, 2, 4]]);
  });

  test('without', () => {
    assertDiff(
      [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      [0, 1, 2, 3, 4, 5, 'a', 'b', 'c', 'd', 8, 9, 10],
      [[6, 2, 4, 6]]);
  });

  test('mix 1', () => {
    assertDiff(
      [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      [0, 'a', 1, 'b', 3, 'c', 4, 6, 7, 'e', 'f', 10],
      [[1, 0, 1, 1], [2, 1, 1, 3], [4, 0, 1, 5], [5, 1, 0, 0], [8, 2, 2, 9]]);
  });

  test('reverse', () => {
    assertDiff(
      [0, 1, 2, 3, 4, 5, 6, 7],
      [7, 6, 5, 4, 3, 2, 1, 0],
      [[0, 3, 4, 0], [4, 4, 3, 5]]);
  });
});
