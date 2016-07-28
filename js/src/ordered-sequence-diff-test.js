// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {suite, suiteSetup, suiteTeardown, test} from 'mocha';
import {assert} from 'chai';
import {MetaTuple, newSetMetaSequence, OrderedKey} from './meta-sequence.js';
import {default as diff, fastForward} from './ordered-sequence-diff.js';
import {default as Set, newSetLeafSequence} from './set.js';
import Ref from './ref.js';
import {smallTestChunks, normalProductionChunks} from './rolling-value-hasher.js';

suite('OrderedSequence', () => {
  suiteSetup(() => {
    smallTestChunks();
  });

  suiteTeardown(() => {
    normalProductionChunks();
  });

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

  test('diff with meta node gap', async () => {
    const newSetSequenceMt = values => {
      const seq = newSetLeafSequence(null, values);
      const set = Object.create(Set.prototype);
      set.sequence = seq;
      return new MetaTuple(
          new Ref(set), new OrderedKey(values[values.length - 1]), values.length, set);
    };

    const m1 = newSetSequenceMt([1, 2]);
    const m2 = newSetSequenceMt([3, 4]);
    const m3 = newSetSequenceMt([5, 6]);
    const s1 = newSetMetaSequence(null, [m1, m3]);
    const s2 = newSetMetaSequence(null, [m1, m2, m3]);

    let [add, rem, mod] = await diff(s1, s2);
    assert.deepEqual([3, 4], add);
    assert.deepEqual([], rem);
    assert.deepEqual([], mod);

    [add, rem, mod] = await diff(s2, s1);
    assert.deepEqual([], add);
    assert.deepEqual([3, 4], rem);
    assert.deepEqual([], mod);
  });
});
