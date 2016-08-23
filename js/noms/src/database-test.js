// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {suite, test} from 'mocha';
import {makeTestingRemoteBatchStore} from './remote-batch-store.js';
import {emptyHash} from './hash.js';
import {assert} from 'chai';
import Commit from './commit.js';
import Database from './database.js';
import {invariant, notNull} from './assert.js';
import List from './list.js';
import Map from './map.js';
import {encodeValue} from './codec.js';
import NomsSet from './set.js'; // namespace collision with JS Set
import {equals} from './compare.js';

suite('Database', () => {
  test('access', async () => {
    const bs = makeTestingRemoteBatchStore();
    const ds = new Database(bs);
    const input = 'abc';

    const c = encodeValue(input);
    const v1 = await ds.readValue(c.hash);
    assert.equal(null, v1);

    bs.schedulePut(c, new Set());
    bs.flush();

    const v2 = await ds.readValue(c.hash);
    assert.equal('abc', v2);
    await ds.close();
  });

  test('commit', async () => {
    const bs = makeTestingRemoteBatchStore();
    let ds = new Database(bs);
    const datasetID = 'ds1';

    const datasets = await ds.datasets();
    assert.isTrue(datasets.isEmpty());

    // |a|
    const aCommit = new Commit('a');
    const ds2 = await ds.commit(datasetID, aCommit);

    // The old database still still has no head.
    assert.isNull(await ds.head(datasetID));

    // The new database has |a|.
    const aRef = notNull(await ds2.headRef(datasetID));
    assert.isTrue(aCommit.hash.equals(aRef.targetHash));
    assert.strictEqual(1, aRef.height);
    const aCommit1 = notNull(await ds2.head(datasetID));
    assert.strictEqual('a', aCommit1.value);
    ds = ds2;

    // |a| <- |b|
    const bCommit = new Commit('b', new NomsSet([aRef]));
    ds = await ds.commit(datasetID, bCommit);
    const bRef = notNull(await ds.headRef(datasetID));
    assert.isTrue(bCommit.hash.equals(bRef.targetHash));
    assert.strictEqual(2, bRef.height);
    assert.strictEqual('b', notNull(await ds.head(datasetID)).value);

    // |a| <- |b|
    //   \----|c|
    // Should be disallowed.
    const cCommit = new Commit('c');
    let message = '';
    try {
      await ds.commit(datasetID, cCommit);
      throw new Error('not reached');
    } catch (ex) {
      message = ex.message;
    }
    assert.strictEqual('Merge needed', message);
    assert.strictEqual('b', notNull(await ds.head(datasetID)).value);

    // |a| <- |b| <- |d|
    const dCommit = new Commit('d', new NomsSet([bRef]));
    ds = await ds.commit(datasetID, dCommit);
    const dRef = notNull(await ds.headRef(datasetID));
    assert.isTrue(dCommit.hash.equals(dRef.targetHash));
    assert.strictEqual(3, dRef.height);
    assert.strictEqual('d', notNull(await ds.head(datasetID)).value);

    // Attempt to recommit |b| with |a| as parent.
    // Should be disallowed.
    try {
      await ds.commit(datasetID, bCommit);
      throw new Error('not reached');
    } catch (ex) {
      message = ex.message;
    }
    // assert.strictEqual('Merge needed', message);
    assert.strictEqual('d', notNull(await ds.head(datasetID)).value);

    // Add a commit to a different datasetId
    ds = await ds.commit('otherDs', aCommit);
    assert.strictEqual('a', notNull(await ds.head('otherDs')).value);

    // Get a fresh database, and verify that both datasets are present
    const newDs = new Database(bs);
    assert.strictEqual('d', notNull(await newDs.head(datasetID)).value);
    assert.strictEqual('a', notNull(await newDs.head('otherDs')).value);
    await ds.close();
  });

  test('concurrency', async () => {
    const bs = makeTestingRemoteBatchStore();
    let ds = new Database(bs);
    const datasetID = 'ds1';

    // |a|
    const aCommit = new Commit('a');
    ds = await ds.commit(datasetID, aCommit);
    const aRef = notNull(await ds.headRef(datasetID));
    const bCommit = new Commit('b', new NomsSet([aRef]));
    ds = await ds.commit(datasetID, bCommit);
    const bRef = notNull(await ds.headRef(datasetID));
    assert.strictEqual('b', notNull(await ds.head(datasetID)).value);

    // Important to create this here.
    let ds2 = new Database(bs);

    // Change 1:
    // |a| <- |b| <- |c|
    const cCommit = new Commit('c', new NomsSet([bRef]));
    ds = await ds.commit(datasetID, cCommit);
    assert.strictEqual('c', notNull(await ds.head(datasetID)).value);

    // Change 2:
    // |a| <- |b| <- |e|
    // Should be disallowed, Database returned by Commit() should have |c| as Head.
    const eCommit = new Commit('e', new NomsSet([bRef]));
    let message = '';
    try {
      ds2 = await ds2.commit(datasetID, eCommit);
      throw new Error('not reached');
    } catch (ex) {
      message = ex.message;
    }
    assert.strictEqual('Merge needed', message);
    assert.strictEqual('c', notNull(await ds.head(datasetID)).value);
    await ds.close();
  });


  test('empty datasets', async () => {
    const ds = new Database(makeTestingRemoteBatchStore());
    const datasets = await ds.datasets();
    assert.strictEqual(0, datasets.size);
    await ds.close();
  });

  test('head', async () => {
    const bs = makeTestingRemoteBatchStore();
    let ds = new Database(bs);

    const commit = new Commit('foo');

    const commitRef = ds.writeValue(commit);
    const datasets = new Map([['foo', commitRef]]);
    const rootRef = ds.writeValue(datasets).targetHash;
    assert.isTrue(await bs.updateRoot(rootRef, emptyHash));
    ds = new Database(bs); // refresh the datasets

    assert.strictEqual(1, datasets.size);
    const fooHead = await ds.head('foo');
    invariant(fooHead);
    assert.isTrue(equals(fooHead, commit));
    const barHead = await ds.head('bar');
    assert.isNull(barHead);
    await ds.close();
  });

  test('height of refs', async () => {
    const ds = new Database(new makeTestingRemoteBatchStore());

    const v1 = ds.writeValue('hello');
    assert.strictEqual(1, v1.height);

    const r1 = ds.writeValue(v1);
    assert.strictEqual(2, r1.height);
    assert.strictEqual(3, ds.writeValue(r1).height);
    await ds.close();
  });

  test('height of collections', async() => {
    const ds = new Database(new makeTestingRemoteBatchStore());

    // Set<String>.
    const v1 = 'hello';
    const v2 = 'world';
    const s1 = new NomsSet([v1, v2]);
    assert.strictEqual(1, ds.writeValue(s1).height);

    // Set<Ref<String>>.
    const s2 = new NomsSet([ds.writeValue(v1), ds.writeValue(v2)]);
    assert.strictEqual(2, ds.writeValue(s2).height);

    // List<Set<String>>.
    const v3 = 'foo';
    const v4 = 'bar';
    const s3 = new NomsSet([v3, v4]);
    const l1 = new List([s1, s3]);
    assert.strictEqual(1, ds.writeValue(l1).height);

    // List<Ref<Set<String>>.
    const l2 = new List([ds.writeValue(s1), ds.writeValue(s3)]);
    assert.strictEqual(2, ds.writeValue(l2).height);

    // List<Ref<Set<Ref<String>>>.
    const s4 = new NomsSet([ds.writeValue(v3), ds.writeValue(v4)]);
    const l3 = new List([ds.writeValue(s4)]);
    assert.strictEqual(3, ds.writeValue(l3).height);

    // List<Set<String> | Ref<Set<String>>>.
    const l4 = new List([s1, ds.writeValue(s3)]);
    assert.strictEqual(2, ds.writeValue(l4).height);
    const l5 = new List([ds.writeValue(s1), s3]);
    assert.strictEqual(2, ds.writeValue(l5).height);
    await ds.close();
  });
});
