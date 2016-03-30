// @flow

import {suite, test} from 'mocha';
import MemoryStore from './memory-store.js';
import {assert} from 'chai';
import Dataset from './dataset.js';
import DataStore from './data-store.js';
import {invariant, notNull} from './assert.js';

suite('Dataset', () => {
  test('commit', async () => {
    const ms = new MemoryStore();
    const store = new DataStore(ms);
    let ds = new Dataset(store, 'ds1');

    // |a|
    const ds2 = await ds.commit('a');

    // The old dataset still still has no head.
    assert.isNull(await ds.head());

    // The new dataset has |a|.
    const aCommit = notNull(await ds2.head());
    assert.strictEqual('a', aCommit.value);
    ds = ds2;

    // |a| <- |b|
    ds = await ds.commit('b', [aCommit.ref]);
    assert.strictEqual('b', notNull(await ds.head()).value);

    // |a| <- |b|
    //   \----|c|
    // Should be disallowed.
    let ex;
    try {
      await ds.commit('c', [aCommit.ref]);
    } catch (e) {
      ex = e;
    }
    invariant(ex instanceof Error);
    assert.strictEqual('Merge needed', ex.message);
    const bCommit = notNull(await ds.head());
    assert.strictEqual('b', bCommit.value);

    // |a| <- |b| <- |d|
    ds = await ds.commit('d');
    assert.strictEqual('d', notNull(await ds.head()).value);


    // Add a commit to a different datasetId
    ds = new Dataset(store, 'otherDs');
    ds = await ds.commit('a');
    assert.strictEqual('a', notNull(await ds.head('otherDs')).value);

    // Get a fresh datastore, and verify that both datasets are present
    const newStore = new DataStore(ms);
    assert.strictEqual('d', notNull(await newStore.head('ds1')).value);
    assert.strictEqual('a', notNull(await newStore.head('otherDs')).value);
  });
});
