// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {suite, test} from 'mocha';
import {assert} from 'chai';
import {fastForward} from './ordered-sequence-diff.js';
import Set from './set.js';

suite('OrderedSequenceCursor', () => {
  test('LONG: fastForward', async () => {
    const numbersFromToBy = (from, to, by) => {
      const res = [];
      for (let i = from; i < to; i += by) {
        res.push(i);
      }
      return res;
    };

    const newMetaSequenceCursor = nums => {
      const lst = new Set(nums);
      assert.isTrue(lst.sequence.isMeta);
      return lst.sequence.newCursorAt();
    };

    {
      // Identical.
      const cur1 = await newMetaSequenceCursor(numbersFromToBy(0, 1000, 1));
      const cur2 = await newMetaSequenceCursor(numbersFromToBy(0, 1000, 1));
      await fastForward(cur1, cur2);
      assert.isFalse(cur1.valid);
      assert.isFalse(cur2.valid);
      await fastForward(cur1, cur2);
      assert.isFalse(cur1.valid);
      assert.isFalse(cur2.valid);
    }

    {
      // A single common prefix.
      const cur1 = await newMetaSequenceCursor(numbersFromToBy(0, 1000, 1));
      const cur2 = await newMetaSequenceCursor(numbersFromToBy(0, 500, 1));
      await fastForward(cur1, cur2);
      assert.deepEqual(500, cur1.getCurrent());
      assert.isFalse(cur2.valid);
      await fastForward(cur1, cur2);
      assert.deepEqual(500, cur1.getCurrent());
      assert.isFalse(cur2.valid);
    }

    {
      // Disjoint.
      const cur1 = await newMetaSequenceCursor(numbersFromToBy(0, 1000, 2));
      const cur2 = await newMetaSequenceCursor(numbersFromToBy(1, 1000, 2));
      for (let i = 0; i < 1000 - 2; i += 2) {
        await fastForward(cur1, cur2);
        assert.deepEqual(i, cur1.getCurrent());
        assert.deepEqual(i + 1, cur2.getCurrent());
        await cur1.advance();
        assert.deepEqual(i + 2, cur1.getCurrent());
        assert.deepEqual(i + 1, cur2.getCurrent());
        await cur2.advance();
      }
    }

    {
      // Trees of different depths.
      const cur1 = await newMetaSequenceCursor(numbersFromToBy(0, 10000, 1));
      const cur2 = await newMetaSequenceCursor(numbersFromToBy(500, 600, 1));
      assert.isFalse(cur1.depth === cur2.depth,
                     `Depths must be different, but both are ${cur1.depth}`);
      await fastForward(cur1, cur2);
      assert.deepEqual(0, cur1.getCurrent());
      assert.deepEqual(500, cur2.getCurrent());
      for (let i = 0; i < 500; i++) {
        await cur1.advance();
      }
      assert.deepEqual(500, cur1.getCurrent());
      await fastForward(cur1, cur2);
      assert.deepEqual(600, cur1.getCurrent());
      assert.isFalse(cur2.valid);
    }
  });
});
