// @flow

import {suite, test} from 'mocha';
import MemoryStore from './memory-store.js';
import {emptyRef} from './ref.js';
import {assert} from 'chai';
import {default as DataStore, getDatasTypes, newCommit} from './data-store.js';
import {invariant, notNull} from './assert.js';
import {newMap} from './map.js';
import {stringType} from './type.js';
import {getRef} from './get-ref.js';
import {encodeNomsValue} from './encode.js';

suite('DataStore', () => {
  test('access', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const input = 'abc';

    const c = encodeNomsValue(input, stringType);
    const v1 = await ds.readValue(c.ref);
    assert.equal(null, v1);

    ms.put(c);
    const v2 = await ds.readValue(c.ref);
    assert.equal('abc', v2);
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
    const aRef = notNull(await ds2.headRef(datasetID));
    assert.isTrue(aCommit.ref.equals(aRef.targetRef));
    const aCommit1 = notNull(await ds2.head(datasetID));
    assert.strictEqual('a', aCommit1.value);
    ds = ds2;

    // |a| <- |b|
    const bCommit = await newCommit('b', [aRef]);
    ds = await ds.commit(datasetID, bCommit);
    const bRef = notNull(await ds.headRef(datasetID));
    assert.isTrue(bCommit.ref.equals(bRef.targetRef));
    assert.strictEqual('b', notNull(await ds.head(datasetID)).value);

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
    assert.strictEqual('b', notNull(await ds.head(datasetID)).value);

    // |a| <- |b| <- |d|
    const dCommit = await newCommit('d', [bRef]);
    ds = await ds.commit(datasetID, dCommit);
    const dRef = notNull(await ds.headRef(datasetID));
    assert.isTrue(dCommit.ref.equals(dRef.targetRef));
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

    // Get a fresh datastore, and verify that both datasets are present
    const newDs = new DataStore(ms);
    assert.strictEqual('d', notNull(await newDs.head(datasetID)).value);
    assert.strictEqual('a', notNull(await newDs.head('otherDs')).value);
  });

  test('concurrency', async () => {
    const ms = new MemoryStore();
    let ds = new DataStore(ms);
    const datasetID = 'ds1';

    // |a|
    const aCommit = await newCommit('a');
    ds = await ds.commit(datasetID, aCommit);
    const aRef = notNull(await ds.headRef(datasetID));
    const bCommit = await newCommit('b', [aRef]);
    ds = await ds.commit(datasetID, bCommit);
    const bRef = notNull(await ds.headRef(datasetID));
    assert.strictEqual('b', notNull(await ds.head(datasetID)).value);

    // Important to create this here.
    let ds2 = new DataStore(ms);

    // Change 1:
    // |a| <- |b| <- |c|
    const cCommit = await newCommit('c', [bRef]);
    ds = await ds.commit(datasetID, cCommit);
    assert.strictEqual('c', notNull(await ds.head(datasetID)).value);

    // Change 2:
    // |a| <- |b| <- |e|
    // Should be disallowed, DataStore returned by Commit() should have |c| as Head.
    const eCommit = await newCommit('e', [bRef]);
    let message = '';
    try {
      ds2 = await ds2.commit(datasetID, eCommit);
      throw new Error('not reached');
    } catch (ex) {
      message = ex.message;
    }
    assert.strictEqual('Merge needed', message);
    assert.strictEqual('c', notNull(await ds.head(datasetID)).value);
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

    const commitRef = ds.writeValue(commit);
    const datasets = await newMap(['foo', commitRef], types.commitMapType);
    const rootRef = ds.writeValue(datasets).targetRef;
    assert.isTrue(await ms.updateRoot(rootRef, emptyRef));
    ds = new DataStore(ms); // refresh the datasets

    assert.strictEqual(1, datasets.size);
    const fooHead = await ds.head('foo');
    invariant(fooHead);
    assert.isTrue(fooHead.equals(commit));
    const barHead = await ds.head('bar');
    assert.isNull(barHead);
  });

  test('writeValue primitives', async () => {
    const ds = new DataStore(new MemoryStore());

    const r1 = ds.writeValue('hello').targetRef;
    const r2 = ds.writeValue(false).targetRef;
    const r3 = ds.writeValue(2).targetRef;

    const v1 = await ds.readValue(r1);
    assert.equal('hello', v1);
    const v2 = await ds.readValue(r2);
    assert.equal(false, v2);
    const v3 = await ds.readValue(r3);
    assert.equal(2, v3);
  });

  test('caching', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms, 1e6);

    const r1 = ds.writeValue('hello').targetRef;
    (ms: any).get = (ms: any).put = () => { assert.fail('unreachable'); };
    const v1 = await ds.readValue(r1);
    assert.equal(v1, 'hello');
    const r2 = ds.writeValue('hello').targetRef;
    assert.isTrue(r1.equals(r2));
  });

  test('caching eviction', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms, 15);

    const r1 = ds.writeValue('hello').targetRef;
    const r2 = ds.writeValue('world').targetRef;
    (ms: any).get = () => { throw new Error(); };
    const v2 = await ds.readValue(r2);
    assert.equal(v2, 'world');
    let ex;
    try {
      await ds.readValue(r1);
    } catch (e) {
      ex = e;
    }
    assert.instanceOf(ex, Error);
  });

  test('caching has', async () => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms, 1e6);

    const r1 = getRef('hello', stringType);
    const v1 = await ds.readValue(r1);
    assert.equal(v1, null);
    (ms: any).get = (ms: any).has = () => { assert.fail('unreachable'); };
    const v2 = await ds.readValue(r1);
    assert.equal(v2, null);
  });
});
