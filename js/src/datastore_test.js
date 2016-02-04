// @flow

import {suite, test} from 'mocha';

import Chunk from './chunk.js';
import MemoryStore from './memory_store.js';
import Ref from './ref.js';
import Struct from './struct.js';
import {assert} from 'chai';
import {DataStore, getDatasTypes} from './datastore.js';
import {invariant} from './assert.js';
import {newMap} from './map.js';
import {newSet} from './set.js';
import {writeValue} from './encode.js';

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

  test('empty datasets', async() => {
    const ms = new MemoryStore();
    const ds = new DataStore(ms);
    const datasets = await ds.datasets();
    assert.strictEqual(0, datasets.size);
  });

  test('head', async() => {
    const ms = new MemoryStore();
    let ds = new DataStore(ms);
    const types = getDatasTypes();

    const commit = new Struct(types.commitType, types.commitTypeDef, {
      value: 'foo',
      parents: await newSet(types.commitSetType, []),
    });

    const commitRef = writeValue(commit, commit.type, ms);
    const datasets = await newMap(types.commitMapType, ['foo', commitRef]);
    const rootRef = writeValue(datasets, datasets.type, ms);
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
