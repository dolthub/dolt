// @flow

import {suite, test} from 'mocha';

import Chunk from './chunk.js';
import MemoryStore from './memory-store.js';
import Ref from './ref.js';
import {assert} from 'chai';
import {default as DataStore, getDatasTypes, newCommit} from './data-store.js';
import {invariant, notNull} from './assert.js';
import {newMap} from './map.js';

suite('DataStore', () => {
  test('access', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const input = 'abc';

    const c = Chunk.fromString(input);
    let c1 = await ds.get(c.ref);
    assert.isTrue(c1.isEmpty());

    let has = await ds.has(c.ref);
    assert.isFalse(has);

    ms.put(c);
    c1 = await ds.get(c.ref);
    assert.isFalse(c1.isEmpty());

    has = await ds.has(c.ref);
    assert.isTrue(has);
  });

  test('commit', async () => {
    const ms = new MemoryStore();
    let ds = new DataStore(ms);
    const datasetID = 'ds1';

    const datasets = await ds.datasets();
    assert.isTrue(datasets.isEmpty());

    // |a|
    const aCommit = await newCommit('a');
    const ds2 = await ds.commit(datasetID, aCommit);

    // The old datastore still still has no head.
    assert.isNull(await ds.head(datasetID));

    // The new datastore has |a|.
    const aCommit1 = notNull(await ds2.head(datasetID));
    assert.strictEqual('a', aCommit1.get('value'));
    ds = ds2;

    // |a| <- |b|
    const bCommit = await newCommit('b', [aCommit.ref]);
    ds = await ds.commit(datasetID, bCommit);
    assert.strictEqual('b', notNull(await ds.head(datasetID)).get('value'));

    // |a| <- |b|
    //   \----|c|
    // Should be disallowed.
    const cCommit = await newCommit('c');
    let message = '';
    try {
      await ds.commit(datasetID, cCommit);
      throw new Error('not reached');
    } catch (ex) {
      message = ex.message;
    }
    assert.strictEqual('Merge needed', message);
    assert.strictEqual('b', notNull(await ds.head(datasetID)).get('value'));

    // |a| <- |b| <- |d|
    const dCommit = await newCommit('d', [bCommit.ref]);
    ds = await ds.commit(datasetID, dCommit);
    assert.strictEqual('d', notNull(await ds.head(datasetID)).get('value'));

    // Attempt to recommit |b| with |a| as parent.
    // Should be disallowed.
    try {
      await ds.commit(datasetID, bCommit);
      throw new Error('not reached');
    } catch (ex) {
      message = ex.message;
    }
    // assert.strictEqual('Merge needed', message);
    assert.strictEqual('d', notNull(await ds.head(datasetID)).get('value'));

    // Add a commit to a different datasetId
    ds = await ds.commit('otherDs', aCommit);
    assert.strictEqual('a', notNull(await ds.head('otherDs')).get('value'));

    // Get a fresh datastore, and verify that both datasets are present
    const newDs = new DataStore(ms);
    assert.strictEqual('d', notNull(await newDs.head(datasetID)).get('value'));
    assert.strictEqual('a', notNull(await newDs.head('otherDs')).get('value'));
  });

  test('concurrency', async () => {
    const ms = new MemoryStore();
    let ds = new DataStore(ms);
    const datasetID = 'ds1';

    // |a|
    const aCommit = await newCommit('a');
    ds = await ds.commit(datasetID, aCommit);
    const bCommit = await newCommit('b', [aCommit.ref]);
    ds = await ds.commit(datasetID, bCommit);
    assert.strictEqual('b', notNull(await ds.head(datasetID)).get('value'));

    // Important to create this here.
    let ds2 = new DataStore(ms);

    // Change 1:
    // |a| <- |b| <- |c|
    const cCommit = await newCommit('c', [bCommit.ref]);
    ds = await ds.commit(datasetID, cCommit);
    assert.strictEqual('c', notNull(await ds.head(datasetID)).get('value'));

    // Change 2:
    // |a| <- |b| <- |e|
    // Should be disallowed, DataStore returned by Commit() should have |c| as Head.
    const eCommit = await newCommit('e', [bCommit.ref]);
    let message = '';
    try {
      ds2 = await ds2.commit(datasetID, eCommit);
      throw new Error('not reached');
    } catch (ex) {
      message = ex.message;
    }
    assert.strictEqual('Merge needed', message);
    assert.strictEqual('c', notNull(await ds.head(datasetID)).get('value'));
  });


  test('empty datasets', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const datasets = await ds.datasets();
    assert.strictEqual(0, datasets.size);
  });

  test('head', async () => {
    const ms = new MemoryStore();
    let ds = new DataStore(ms);
    const types = getDatasTypes();

    const commit = await newCommit('foo', []);

    const commitRef = ds.writeValue(commit, commit.type);
    const datasets = await newMap(['foo', commitRef], types.commitMapType);
    const rootRef = ds.writeValue(datasets, datasets.type);
    assert.isTrue(await ms.updateRoot(rootRef, new Ref()));
    ds = new DataStore(ms); // refresh the datasets

    assert.strictEqual(1, datasets.size);
    const fooHead = await ds.head('foo');
    invariant(fooHead);
    assert.isTrue(fooHead.equals(commit));
    const barHead = await ds.head('bar');
    assert.isNull(barHead);
  });
});
