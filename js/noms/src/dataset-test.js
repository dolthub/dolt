// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {suite, test} from './jest.js';
import makeRemoteBatchStoreFake from './remote-batch-store-fake.js';
import {assert} from 'chai';
import Database from './database.js';
import {invariant} from './assert.js';
import {equals} from './compare.js';

suite('Dataset', () => {
  test('id validation', () => {
    const db = new Database(makeRemoteBatchStoreFake());

    const invalidDatasetNames = [' ', '', 'a ', ' a', '$', '#', ':', '\n', '💩'];
    for (const s of invalidDatasetNames) {
      assert.throws(() => { db.getDataset(s); });
    }
  });

  test('head', async () => {
    const bs = makeRemoteBatchStoreFake();
    let db = new Database(bs);

    const headVal = 'fooContent';
    await db.commit(db.getDataset('foo'), headVal);
    db = new Database(bs); // refresh the datasets

    const ds = await db.getDataset('foo');
    const fooHead = await ds.headValue();
    invariant(fooHead);
    assert.isTrue(equals(fooHead, headVal));

    const ds2 = await db.getDataset('bar');
    const barHead = await ds2.head();
    assert.isNull(barHead);
    await db.close();
  });
});
