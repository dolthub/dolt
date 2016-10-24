// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {suite, test} from 'mocha';
import makeRemoteBatchStoreFake from './remote-batch-store-fake.js';
import {TestingDelegate} from './remote-batch-store-fake.js';
import RemoteBatchStore from './remote-batch-store.js';
import MemoryStore from './memory-store.js';
import {assert} from 'chai';
import Commit from './commit.js';
import Database from './database.js';
import {notNull} from './assert.js';
import List from './list.js';
import Struct, {newStruct} from './struct.js';
import {encodeValue} from './codec.js';
import NomsSet from './set.js'; // namespace collision with JS Set
import {invariant} from './assert.js';

suite('Database', () => {
  test('access', async () => {
    const bs = makeRemoteBatchStoreFake();
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
    const bs = makeRemoteBatchStoreFake();
    const db = new Database(bs);
    let ds = await db.getDataset('ds1');

    const datasets = await db.datasets();
    assert.isTrue(datasets.isEmpty());

    // |a|
    const aCommit = new Commit('a');
    let ds2 = await db.commit(ds, 'a');

    // The old dataset still has no head.
    assert.isNull(await ds.head());

    // The new database has |a|.
    const aRef = notNull(await ds2.headRef());
    assert.isTrue(aCommit.hash.equals(aRef.targetHash));
    assert.strictEqual(1, aRef.height);
    const aCommit1 = notNull(await ds2.head());
    assert.strictEqual('a', aCommit1.value);

    // |a| <- |b|
    ds = await db.commit(ds2, 'b', {parents: [aRef]});
    const bRef = notNull(await ds.headRef());
    assert.strictEqual(2, bRef.height);
    assert.strictEqual('b', notNull(await ds.head()).value);

    // |a| <- |b|
    //   \----|c|
    // Should be disallowed.
    let message = '';
    try {
      ds = await db.commit(ds2, 'c');
      throw new Error('not reached');
    } catch (ex) {
      message = ex.message;
    }
    assert.strictEqual('Merge needed', message);
    assert.strictEqual('b', notNull(await ds.head()).value);

    // |a| <- |b| <- |d|
    ds = await db.commit(ds, 'd', {parents: [bRef]});
    const dRef = notNull(await ds.headRef());
    assert.strictEqual(3, dRef.height);
    assert.strictEqual('d', notNull(await ds.head()).value);

    // Attempt to recommit |b| with |a| as parent.
    // Should be disallowed.
    try {
      ds = await db.commit(ds, 'b', {parents: [aRef]});
      throw new Error('not reached');
    } catch (ex) {
      message = ex.message;
    }
    // assert.strictEqual('Merge needed', message);
    assert.strictEqual('d', notNull(await ds.head()).value);

    // Add a commit to a different datasetId
    ds2 = await db.getDataset('otherDs');
    ds2 = await db.commit(ds2, 'a');
    assert.strictEqual('a', notNull(await ds2.head()).value);

    // Get a fresh database, and verify that both datasets are present
    const newDB = new Database(bs);
    const newDS1 = await newDB.getDataset('ds1');
    const newDS2 = await newDB.getDataset('otherDs');
    assert.strictEqual('d', notNull(await newDS1.head()).value);
    assert.strictEqual('a', notNull(await newDS2.head()).value);
    await db.close();
  });

  test('duplicate commit', async () => {
    const bs = makeRemoteBatchStoreFake();
    const db = new Database(bs);
    const ds = await db.getDataset('ds1');

    const datasets = await db.datasets();
    assert.isTrue(datasets.isEmpty());

    await db.commit(ds, 'a');
    // Should be disallowed, Dataset returned by Commit() should have |c| as Head.
    let message = '';
    try {
      await db.commit(ds, 'a');
      throw new Error('not reached');
    } catch (ex) {
      message = ex.message;
    }
    assert.strictEqual('Merge needed', message);

    await db.close();
  });

  test('concurrency', async () => {
    const bs = makeRemoteBatchStoreFake();
    const db = new Database(bs);
    let ds = await db.getDataset('ds1');

    // |a| <- |b|
    ds = await db.commit(ds, 'a');
    const aRef = notNull(await ds.headRef());
    ds = await db.commit(ds, 'b', {parents: [aRef]});
    const bRef = notNull(await ds.headRef());
    assert.strictEqual('b', notNull(await ds.head()).value);

    // Important to create this here.
    const db2 = new Database(bs);

    // Change 1:
    // |a| <- |b| <- |c|
    ds = await db.commit(ds, 'c', {parents: [bRef]});
    assert.strictEqual('c', notNull(await ds.head()).value);

    // Change 2:
    // |a| <- |b| <- |e|
    // Should be disallowed, Dataset returned by Commit() should have |c| as Head.
    let message = '';
    try {
      await db2.commit(ds, 'e', {parents: [bRef]});
      throw new Error('not reached');
    } catch (ex) {
      message = ex.message;
    }
    assert.strictEqual('Merge needed', message);
    assert.strictEqual('c', notNull(await ds.head()).value);
    await db.close();
  });

  test('commit with concurrent chunk store use', async () => {
    const dsID = 'ds1';
    const ms = new MemoryStore();

    // Craft DB that will allow me to move the backing ChunkStore while the code isn't looking
    const hookedDelegate = new TestingDelegate(ms);
    const hookedDB = new Database(new RemoteBatchStore(3, hookedDelegate));

    let ds1 = await hookedDB.getDataset(dsID);
    ds1 = await hookedDB.commit(ds1, 'a');
    ds1 = await hookedDB.commit(ds1, 'b');
    assert.strictEqual('b', notNull(await ds1.headValue()));

    // Concurrent change, but to some other dataset. This shouldn't stop changes to ds1.
    // ds1: |a| <- |b|
    // ds2: |stuff|
    hookedDelegate.preUpdateRootHook = async () => {
      const db = new Database(new RemoteBatchStore(3, new TestingDelegate(ms)));
      const ds2 = await db.commit(db.getDataset('ds2'), 'stuff');
      assert.strictEqual(notNull(await ds2.headValue()), 'stuff');
      hookedDelegate.preUpdateRootHook = () => Promise.resolve();
    };

    // Attempted Concurrent change, which should proceed without a problem
    ds1 = await hookedDB.commit(ds1, 'c');
    assert.strictEqual(notNull(await ds1.headValue()), 'c');

    // Concurrent change, to move root out from under my feet:
    // ds1: |a| <- |b| <- |c| <- |e|
    hookedDelegate.preUpdateRootHook = async () => {
      const db = new Database(new RemoteBatchStore(3, new TestingDelegate(ms)));
      const ds = await db.commit(db.getDataset(dsID), 'e');
      assert.strictEqual(notNull(await ds.headValue()), 'e');
      hookedDelegate.preUpdateRootHook = () => Promise.resolve();
    };

    // Attempted Concurrent change, which should fail due to the above
    let message = '';
    try {
      await hookedDB.commit(ds1, 'nope');
      throw new Error('not reached');
    } catch (ex) {
      message = ex.message;
    }
    assert.strictEqual('Merge needed', message);
    assert.strictEqual(notNull(await hookedDB.getDataset(dsID).headValue()), 'e');
  });

  test('empty datasets', async () => {
    const ds = new Database(makeRemoteBatchStoreFake());
    const datasets = await ds.datasets();
    assert.strictEqual(0, datasets.size);
    await ds.close();
  });

  test('height of refs', async () => {
    const ds = new Database(new makeRemoteBatchStoreFake());

    const v1 = ds.writeValue('hello');
    assert.strictEqual(1, v1.height);

    const r1 = ds.writeValue(v1);
    assert.strictEqual(2, r1.height);
    assert.strictEqual(3, ds.writeValue(r1).height);
    await ds.close();
  });

  test('height of collections', async() => {
    const ds = new Database(new makeRemoteBatchStoreFake());

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

  test('meta option', async () => {
    const db = new Database(makeRemoteBatchStoreFake());
    let ds = await db.getDataset('ds1');

    const meta = newStruct('M', {author: 'arv'});
    ds = await db.commit(ds, 'a', {meta});

    const c = await ds.head();
    invariant(c instanceof Struct);

    const m2 = c.meta;
    assert.equal(m2.author, 'arv');
  });
});
