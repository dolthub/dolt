// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {assert} from 'chai';
import {suite, test} from 'mocha';

import List from './list.js';
import {
  OrderedKey,
  MetaTuple,
  newOrderedMetaSequenceChunkFn,
  newIndexedMetaSequenceChunkFn,
} from './meta-sequence.js';
import Ref from './ref.js';
import Set from './set.js';
import {Kind} from './noms-kind.js';

suite('MetaSequence', () => {
  test('calculate ordered sequence MetaTuple height', async () => {
    const set1 = new Set(['bar', 'baz']);
    const set2 = new Set(['foo', 'qux', 'zoo']);
    const mt1 = new MetaTuple(new Ref(set1), new OrderedKey('baz'), 2, set1);
    const mt2 = new MetaTuple(new Ref(set2), new OrderedKey('zoo'), 3, set2);

    assert.strictEqual(1, mt1.ref.height);
    assert.strictEqual(1, mt2.ref.height);

    let [col, key, numLeaves] = newOrderedMetaSequenceChunkFn(Kind.Set, null)([mt1, mt2]);
    const oseq1 = new MetaTuple(new Ref(col), key, numLeaves, null);
    assert.strictEqual(2, oseq1.ref.height);

    // At this point the sequence isn't really valid because I'm reusing a MetaNode, which isn't
    // allowed (the values are now out of order). For the purpose of testing height, it's fine.
    [col, key, numLeaves] = newOrderedMetaSequenceChunkFn(Kind.Set, null)([oseq1, oseq1]);
    const oseq2 = new MetaTuple(new Ref(col), key, numLeaves, null);
    assert.strictEqual(3, oseq2.ref.height);
  });

  test('calculate indexed sequence MetaTuple height', async () => {
    const list1 = new List(['bar', 'baz']);
    const list2 = new List(['foo', 'qux', 'zoo']);
    const mt1 = new MetaTuple(new Ref(list1), new OrderedKey(2), 2, list1);
    const mt2 = new MetaTuple(new Ref(list2), new OrderedKey(3), 3, list2);

    assert.strictEqual(1, mt1.ref.height);
    assert.strictEqual(1, mt2.ref.height);

    let [col, key, numLeaves] = newIndexedMetaSequenceChunkFn(Kind.List, null, null)([mt1, mt2]);
    const iseq1 = new MetaTuple(new Ref(col), key, numLeaves, null);
    assert.strictEqual(2, iseq1.ref.height);
    [col, key, numLeaves] = newIndexedMetaSequenceChunkFn(Kind.List, null, null)([iseq1, iseq1]);
    const iseq2 = new MetaTuple(new Ref(col), key, numLeaves, null);
    assert.strictEqual(3, iseq2.ref.height);
  });
});
