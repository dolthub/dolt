// @flow

import {suite, test} from 'mocha';
import MemoryStore from './memory-store.js';
import {emptyRef} from './ref.js';
import {assert} from 'chai';
import {default as Database, getDatasTypes, newCommit} from './database.js';
import {invariant, notNull} from './assert.js';
import {newMap} from './map.js';
import {stringType} from './type.js';
import {getRef} from './get-ref.js';
import {encodeNomsValue} from './encode.js';

suite('Database', () => {
  test('access', async () => {
    const ms = new MemoryStore();
    const db = new Database(ms);
    const input = 'abc';

    const c = encodeNomsValue(input, stringType);
    const v1 = await db.readValue(c.ref);
    assert.equal(null, v1);

    ms.put(c);
    const v2 = await db.readValue(c.ref);
    assert.equal('abc', v2);
  });

  test('commit', async () => {
    const ms = new MemoryStore();
    let db = new Database(ms);
    const datasetID = 'ds1';

    const datasets = await db.datasets();
    assert.isTrue(datasets.isEmpty());

    // |a|
    const aCommit = await newCommit('a');
    const db2 = await db.commit(datasetID, aCommit);

    // The old database still still has no head.
    assert.isNull(await db.head(datasetID));

    // The new database has |a|.
    const aRef = notNull(await db2.headRef(datasetID));
    assert.isTrue(aCommit.ref.equals(aRef.targetRef));
    const aCommit1 = notNull(await db2.head(datasetID));
    assert.strictEqual('a', aCommit1.value);
    db = db2;

    // |a| <- |b|
    const bCommit = await newCommit('b', [aRef]);
    db = await db.commit(datasetID, bCommit);
    const bRef = notNull(await db.headRef(datasetID));
    assert.isTrue(bCommit.ref.equals(bRef.targetRef));
    assert.strictEqual('b', notNull(await db.head(datasetID)).value);

    // |a| <- |b|
    //   \----|c|
    // Should be disallowed.
    const cCommit = await newCommit('c');
    let message = '';
    try {
      await db.commit(datasetID, cCommit);
      throw new Error('not reached');
    } catch (ex) {
      message = ex.message;
    }
    assert.strictEqual('Merge needed', message);
    assert.strictEqual('b', notNull(await db.head(datasetID)).value);

    // |a| <- |b| <- |d|
    const dCommit = await newCommit('d', [bRef]);
    db = await db.commit(datasetID, dCommit);
    const dRef = notNull(await db.headRef(datasetID));
    assert.isTrue(dCommit.ref.equals(dRef.targetRef));
    assert.strictEqual('d', notNull(await db.head(datasetID)).value);

    // Attempt to recommit |b| with |a| as parent.
    // Should be disallowed.
    try {
      await db.commit(datasetID, bCommit);
      throw new Error('not reached');
    } catch (ex) {
      message = ex.message;
    }
    // assert.strictEqual('Merge needed', message);
    assert.strictEqual('d', notNull(await db.head(datasetID)).value);

    // Add a commit to a different datasetId
    db = await db.commit('otherDb', aCommit);
    assert.strictEqual('a', notNull(await db.head('otherDb')).value);

    // Get a fresh database, and verify that both datasets are present
    const newDb = new Database(ms);
    assert.strictEqual('d', notNull(await newDb.head(datasetID)).value);
    assert.strictEqual('a', notNull(await newDb.head('otherDb')).value);
  });

  test('concurrency', async () => {
    const ms = new MemoryStore();
    let db = new Database(ms);
    const datasetID = 'ds1';

    // |a|
    const aCommit = await newCommit('a');
    db = await db.commit(datasetID, aCommit);
    const aRef = notNull(await db.headRef(datasetID));
    const bCommit = await newCommit('b', [aRef]);
    db = await db.commit(datasetID, bCommit);
    const bRef = notNull(await db.headRef(datasetID));
    assert.strictEqual('b', notNull(await db.head(datasetID)).value);

    // Important to create this here.
    let db2 = new Database(ms);

    // Change 1:
    // |a| <- |b| <- |c|
    const cCommit = await newCommit('c', [bRef]);
    db = await db.commit(datasetID, cCommit);
    assert.strictEqual('c', notNull(await db.head(datasetID)).value);

    // Change 2:
    // |a| <- |b| <- |e|
    // Should be disallowed, Database returned by Commit() should have |c| as Head.
    const eCommit = await newCommit('e', [bRef]);
    let message = '';
    try {
      db2 = await db2.commit(datasetID, eCommit);
      throw new Error('not reached');
    } catch (ex) {
      message = ex.message;
    }
    assert.strictEqual('Merge needed', message);
    assert.strictEqual('c', notNull(await db.head(datasetID)).value);
  });


  test('empty datasets', async () => {
    const ms = new MemoryStore();
    const db = new Database(ms);
    const datasets = await db.datasets();
    assert.strictEqual(0, datasets.size);
  });

  test('head', async () => {
    const ms = new MemoryStore();
    let db = new Database(ms);
    const types = getDatasTypes();

    const commit = await newCommit('foo', []);

    const commitRef = db.writeValue(commit);
    const datasets = await newMap(['foo', commitRef], types.commitMapType);
    const rootRef = db.writeValue(datasets).targetRef;
    assert.isTrue(await ms.updateRoot(rootRef, emptyRef));
    db = new Database(ms); // refresh the datasets

    assert.strictEqual(1, datasets.size);
    const fooHead = await db.head('foo');
    invariant(fooHead);
    assert.isTrue(fooHead.equals(commit));
    const barHead = await db.head('bar');
    assert.isNull(barHead);
  });

  test('writeValue primitives', async () => {
    const db = new Database(new MemoryStore());

    const r1 = db.writeValue('hello').targetRef;
    const r2 = db.writeValue(false).targetRef;
    const r3 = db.writeValue(2).targetRef;

    const v1 = await db.readValue(r1);
    assert.equal('hello', v1);
    const v2 = await db.readValue(r2);
    assert.equal(false, v2);
    const v3 = await db.readValue(r3);
    assert.equal(2, v3);
  });

  test('caching', async () => {
    const ms = new MemoryStore();
    const db = new Database(ms, 1e6);

    const r1 = db.writeValue('hello').targetRef;
    (ms: any).get = (ms: any).put = () => { assert.fail('unreachable'); };
    const v1 = await db.readValue(r1);
    assert.equal(v1, 'hello');
    const r2 = db.writeValue('hello').targetRef;
    assert.isTrue(r1.equals(r2));
  });

  test('caching eviction', async () => {
    const ms = new MemoryStore();
    const db = new Database(ms, 15);

    const r1 = db.writeValue('hello').targetRef;
    const r2 = db.writeValue('world').targetRef;
    (ms: any).get = () => { throw new Error(); };
    const v2 = await db.readValue(r2);
    assert.equal(v2, 'world');
    let ex;
    try {
      await db.readValue(r1);
    } catch (e) {
      ex = e;
    }
    assert.instanceOf(ex, Error);
  });

  test('caching has', async () => {
    const ms = new MemoryStore();
    const db = new Database(ms, 1e6);

    const r1 = getRef('hello', stringType);
    const v1 = await db.readValue(r1);
    assert.equal(v1, null);
    (ms: any).get = (ms: any).has = () => { assert.fail('unreachable'); };
    const v2 = await db.readValue(r1);
    assert.equal(v2, null);
  });
});
