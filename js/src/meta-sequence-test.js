// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';

import {makeTestingBatchStore} from './batch-store-adaptor.js';
import Database from './database.js';
import {ListLeafSequence} from './list.js';
import {
  makeListType,
  makeSetType,
  stringType,
} from './type.js';
import {MetaTuple, newOrderedMetaSequenceChunkFn, newIndexedMetaSequenceChunkFn,} from
  './meta-sequence.js';
import RefValue from './ref-value.js';
import {SetLeafSequence} from './set.js';

suite('MetaSequence', () => {
  const setOfStringType = makeSetType(stringType);
  const listOfStringType = makeListType(stringType);

  test('calculate ordered sequence MetaTuple height', async () => {
    const ds = new Database(makeTestingBatchStore());

    const seq1 = new SetLeafSequence(ds, setOfStringType, ['bar', 'baz']);
    const seq2 = new SetLeafSequence(ds, setOfStringType, ['foo', 'qux', 'zoo']);
    const mt1 = new MetaTuple(new RefValue(seq1, setOfStringType), 'baz', 2, seq1);
    const mt2 = new MetaTuple(new RefValue(seq2, setOfStringType), 'zoo', 3, seq2);

    assert.strictEqual(1, mt1.ref.height);
    assert.strictEqual(1, mt2.ref.height);

    const oseq1 = newOrderedMetaSequenceChunkFn(setOfStringType)([mt1, mt2])[0];
    assert.strictEqual(2, oseq1.ref.height);

    // At this point the sequence isn't really valid because I'm reusing a MetaNode, which isn't
    // allowed (the values are now out of order). For the purpose of testing height, it's fine.
    const oseq2 = newOrderedMetaSequenceChunkFn(setOfStringType)([oseq1, oseq1])[0];
    assert.strictEqual(3, oseq2.ref.height);
  });

  test('calculate indexed sequence MetaTuple height', async () => {
    const ds = new Database(makeTestingBatchStore());

    const seq1 = new ListLeafSequence(ds, listOfStringType, ['bar', 'baz']);
    const seq2 = new ListLeafSequence(ds, listOfStringType, ['foo', 'qux', 'zoo']);
    const mt1 = new MetaTuple(new RefValue(seq1, listOfStringType), 2, 2, seq1);
    const mt2 = new MetaTuple(new RefValue(seq2, listOfStringType), 3, 3, seq2);

    assert.strictEqual(1, mt1.ref.height);
    assert.strictEqual(1, mt2.ref.height);

    const iseq1 = newIndexedMetaSequenceChunkFn(listOfStringType)([mt1, mt2])[0];
    assert.strictEqual(2, iseq1.ref.height);
    const iseq2 = newIndexedMetaSequenceChunkFn(listOfStringType)([iseq1, iseq1])[0];
    assert.strictEqual(3, iseq2.ref.height);
  });
});
