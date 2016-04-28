// @flow

import {suite, test} from 'mocha';
import {assert} from 'chai';
import {Sequence, SequenceCursor} from './sequence.js';
import {notNull} from './assert.js';
import {makeCompoundType, valueType} from './type.js';
import {Kind} from './noms-kind.js';

class TestSequence extends Sequence<any> {
  constructor(items: Array<any>) {
    super(null, makeCompoundType(Kind.List, valueType), items);
  }

  getChildSequence(idx: number): // eslint-disable-line no-unused-vars
      Promise<?Sequence> {
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

  test('maxNPrevItems with empty sequence.', async () => {
    const cur = testCursor([[]]);
    assert.deepEqual([], await cur.maxNPrevItems(0));
    assert.deepEqual([], await cur.maxNPrevItems(1));
  });

  test('maxNPrevItems with single item sequence.', async () => {
    const cur = testCursor([[100], [101], [102]]);
    assert.deepEqual([], await cur.maxNPrevItems(0));
    assert.deepEqual([], await cur.maxNPrevItems(1));
    assert.deepEqual([], await cur.maxNPrevItems(2));
    assert.deepEqual([], await cur.maxNPrevItems(3));
    assert.strictEqual(0, cur.idx);

    assert.isTrue(await cur.advance());
    assert.deepEqual([], await cur.maxNPrevItems(0));
    assert.deepEqual([100], await cur.maxNPrevItems(1));
    assert.deepEqual([100], await cur.maxNPrevItems(2));
    assert.deepEqual([100], await cur.maxNPrevItems(3));
    assert.strictEqual(0, cur.idx);

    assert.isTrue(await cur.advance());
    assert.deepEqual([], await cur.maxNPrevItems(0));
    assert.deepEqual([101], await cur.maxNPrevItems(1));
    assert.deepEqual([100, 101], await cur.maxNPrevItems(2));
    assert.deepEqual([100, 101], await cur.maxNPrevItems(3));
    assert.strictEqual(0, cur.idx);

    assert.isFalse(await cur.advance());
    assert.deepEqual([102], await cur.maxNPrevItems(1));
    assert.deepEqual([101, 102], await cur.maxNPrevItems(2));
    assert.deepEqual([100, 101, 102], await cur.maxNPrevItems(3));
    assert.deepEqual([100, 101, 102], await cur.maxNPrevItems(4));
    assert.strictEqual(1, cur.idx);
  });

  test('maxNPrevItems with multi-item sequence.', async () => {
    const cur = testCursor([[100, 101, 102, 103], [104, 105, 106, 107]]);
    assert.deepEqual([], await cur.maxNPrevItems(0));
    assert.deepEqual([], await cur.maxNPrevItems(1));
    assert.deepEqual([], await cur.maxNPrevItems(2));
    assert.deepEqual([], await cur.maxNPrevItems(3));
    assert.strictEqual(0, cur.idx);

    assert.isTrue(await cur.advance());
    assert.deepEqual([], await cur.maxNPrevItems(0));
    assert.deepEqual([100], await cur.maxNPrevItems(1));
    assert.deepEqual([100], await cur.maxNPrevItems(2));
    assert.deepEqual([100], await cur.maxNPrevItems(3));
    assert.strictEqual(1, cur.idx);

    assert.isTrue(await cur.advance());
    assert.deepEqual([], await cur.maxNPrevItems(0));
    assert.deepEqual([101], await cur.maxNPrevItems(1));
    assert.deepEqual([100, 101], await cur.maxNPrevItems(2));
    assert.deepEqual([100, 101], await cur.maxNPrevItems(3));
    assert.strictEqual(2, cur.idx);

    assert.isTrue(await cur.advance());
    assert.deepEqual([], await cur.maxNPrevItems(0));
    assert.deepEqual([102], await cur.maxNPrevItems(1));
    assert.deepEqual([101, 102], await cur.maxNPrevItems(2));
    assert.deepEqual([100, 101, 102], await cur.maxNPrevItems(3));
    assert.strictEqual(3, cur.idx);

    assert.isTrue(await cur.advance());
    assert.deepEqual([], await cur.maxNPrevItems(0));
    assert.deepEqual([103], await cur.maxNPrevItems(1));
    assert.deepEqual([102, 103], await cur.maxNPrevItems(2));
    assert.deepEqual([101, 102, 103], await cur.maxNPrevItems(3));
    assert.strictEqual(0, cur.idx);

    assert.isTrue(await cur.advance());
    assert.deepEqual([], await cur.maxNPrevItems(0));
    assert.deepEqual([104], await cur.maxNPrevItems(1));
    assert.deepEqual([103, 104], await cur.maxNPrevItems(2));
    assert.deepEqual([102, 103, 104], await cur.maxNPrevItems(3));
    assert.strictEqual(1, cur.idx);

    assert.isTrue(await cur.advance());
    assert.deepEqual([], await cur.maxNPrevItems(0));
    assert.deepEqual([105], await cur.maxNPrevItems(1));
    assert.deepEqual([104, 105], await cur.maxNPrevItems(2));
    assert.deepEqual([103, 104, 105], await cur.maxNPrevItems(3));
    assert.strictEqual(2, cur.idx);

    assert.isTrue(await cur.advance());
    assert.deepEqual([], await cur.maxNPrevItems(0));
    assert.deepEqual([106], await cur.maxNPrevItems(1));
    assert.deepEqual([105, 106], await cur.maxNPrevItems(2));
    assert.deepEqual([104, 105, 106], await cur.maxNPrevItems(3));
    assert.strictEqual(3, cur.idx);

    assert.deepEqual([100, 101, 102, 103, 104, 105, 106], await cur.maxNPrevItems(7));
    assert.deepEqual([100, 101, 102, 103, 104, 105, 106], await cur.maxNPrevItems(8));

    assert.isFalse(await cur.advance());
    assert.deepEqual([], await cur.maxNPrevItems(0));
    assert.deepEqual([107], await cur.maxNPrevItems(1));
    assert.deepEqual([106, 107], await cur.maxNPrevItems(2));
    assert.deepEqual([105, 106, 107], await cur.maxNPrevItems(3));
    assert.strictEqual(4, cur.idx);

    assert.deepEqual([101, 102, 103, 104, 105, 106, 107], await cur.maxNPrevItems(7));
    assert.deepEqual([100, 101, 102, 103, 104, 105, 106, 107], await cur.maxNPrevItems(8));
    assert.deepEqual([100, 101, 102, 103, 104, 105, 106, 107], await cur.maxNPrevItems(8));
  });
});
