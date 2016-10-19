// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {suite, test} from 'mocha';
import {assert} from 'chai';
import Sequence, {SequenceCursor} from './sequence.js';
import {notNull} from './assert.js';
import {makeListType, valueType} from './type.js';

class TestSequence extends Sequence<any> {
  constructor(items: Array<any>) {
    super(null, makeListType(valueType), items);
  }

  getChildSequence(idx: number): Promise<?Sequence<any>> {
    return Promise.resolve(new TestSequence(this.items[idx]));
  }
}

class TestSequenceCursor extends SequenceCursor<any, TestSequence> {
  clone(): TestSequenceCursor {
    return new TestSequenceCursor(this.parent ? this.parent.clone() : null, this.sequence,
                                  this.idx);
  }
}

suite('SequenceCursor', () => {
  function testCursor(data: Array<any>): TestSequenceCursor {
    const s1 = new TestSequence(data);
    const c1 = new TestSequenceCursor(null, s1, 0);
    const s2 = new TestSequence(data[0]);
    return new TestSequenceCursor(c1, s2, 0);
  }

  function expect(c: TestSequenceCursor, expectIdx: number,
      expectParentIdx: number, expectValid: boolean, expectVal: ?number) {
    assert.strictEqual(expectIdx, c.indexInChunk, 'indexInChunk');
    const parent = notNull(c.parent);
    assert.strictEqual(expectParentIdx, parent.indexInChunk, 'parentIdx');
    assert.strictEqual(expectValid, c.valid, 'valid');
    let actualVal = null;
    if (c.valid) {
      actualVal = c.getCurrent();
    }
    assert.strictEqual(expectVal, actualVal, 'value');
  }

  test('retreating past the start', async () => {
    const cur = testCursor([[100, 101],[102]]);
    expect(cur, 0, 0, true, 100);
    assert.isFalse(await cur.retreat());
    expect(cur, -1, 0, false, null);
    assert.isFalse(await cur.retreat());
    expect(cur, -1, 0, false, null);
  });

  test('retreating past the start, then advanding past the end', async () => {
    const cur = testCursor([[100, 101],[102]]);
    assert.isFalse(await cur.retreat());
    assert.isTrue(await cur.advance());
    expect(cur, 0, 0, true, 100);
    assert.isTrue(await cur.advance());
    expect(cur, 1, 0, true, 101);
    assert.isTrue(await cur.advance());
    expect(cur, 0, 1, true, 102);
    assert.isFalse(await cur.advance());
    expect(cur, 1, 1, false, null);
    assert.isFalse(await cur.advance());
    expect(cur, 1, 1, false, null);
  });

  test('advancing past the end', async () => {
    const cur = testCursor([[100, 101],[102]]);
    assert.isTrue(await cur.advance());
    expect(cur, 1, 0, true, 101);
    assert.isTrue(await cur.retreat());
    expect(cur, 0, 0, true, 100);
    assert.isFalse(await cur.retreat());
    expect(cur, -1, 0, false, null);
    assert.isFalse(await cur.retreat());
    expect(cur, -1, 0, false, null);
  });

  test('advancing past the end, then retreating past the start.', async () => {
    const cur = testCursor([[100, 101],[102]]);
    assert.isTrue(await cur.advance());
    assert.isTrue(await cur.advance());
    expect(cur, 0, 1, true, 102);
    assert.isFalse(await cur.advance());
    expect(cur, 1, 1, false, null);
    assert.isFalse(await cur.advance());
    expect(cur, 1, 1, false, null);
    assert.isTrue(await cur.retreat());
    expect(cur, 0, 1, true, 102);
    assert.isTrue(await cur.retreat());
    expect(cur, 1, 0, true, 101);
    assert.isTrue(await cur.retreat());
    expect(cur, 0, 0, true, 100);
    assert.isFalse(await cur.retreat());
    expect(cur, -1, 0, false, null);
    assert.isFalse(await cur.retreat());
    expect(cur, -1, 0, false, null);
  });
});
