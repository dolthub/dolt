// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {invariant} from './assert.js';
import BatchStoreAdaptor from './batch-store-adaptor.js';
import Dataset from './dataset.js';
import Database from './database.js';
import HttpBatchStore from './http-batch-store.js';
import Hash from './hash.js';
import {DatabaseSpec, DatasetSpec, HashSpec, parseObjectSpec} from './specs.js';
import {assert} from 'chai';
import {suite, test} from 'mocha';

suite('Specs', () => {
  test('DatabaseSpec', async () => {
    const notAllowed = ['mem:', 'mem:stuff', 'http:', 'https:', 'random:', 'random:random'];
    notAllowed.forEach(s => assert.isNull(DatabaseSpec.parse(s)));

    let spec = DatabaseSpec.parse('mem');
    invariant(spec);
    assert.equal(spec.scheme, 'mem');
    assert.equal(spec.path, '');
    let database = spec.database();
    assert.instanceOf(database, Database);
    assert.instanceOf(database._vs._bs, BatchStoreAdaptor);
    await database.close();

    spec = DatabaseSpec.parse('http://foo');
    invariant(spec);
    assert.isNotNull(spec);
    assert.equal(spec.scheme, 'http');
    assert.equal(spec.path, '//foo');
    database = spec.database();
    assert.instanceOf(database, Database);
    assert.instanceOf(database._vs._bs, HttpBatchStore);
    await database.close();

    spec = DatabaseSpec.parse('https://foo');
    invariant(spec);
    assert.isNotNull(spec);
    assert.equal(spec.scheme, 'https');
    assert.equal(spec.path, '//foo');
  });

  test('DatasetSpec', async () => {
    const invalid = ['mem', 'mem:', 'http', 'http:', 'http://foo', 'monkey', 'monkey:balls',
        'http::dsname', 'mem:/a/bogus/path:dsname'];
    invalid.forEach(s => assert.isNull(DatasetSpec.parse(s)));

    const invalidDatasetNames = [' ', '', '$', '#', ':', '\n', '💩'];
    for (const s of invalidDatasetNames) {
      assert.isNull(DatasetSpec.parse(`mem:${s}`));
    }

    const validDatasetNames = ['a', 'Z', '0','/', '-', '_'];
    for (const s of validDatasetNames) {
      assert.isNotNull(DatasetSpec.parse(`mem:${s}`));
    }

    let spec = DatasetSpec.parse('mem:ds');
    invariant(spec);
    assert.equal(spec.name, 'ds');
    assert.equal(spec.database.scheme, 'mem');
    assert.equal(spec.database.path, '');
    let ds = spec.dataset();
    assert.instanceOf(ds, Dataset);
    assert.instanceOf(ds.database._vs._bs, BatchStoreAdaptor);
    await ds.database.close();

    spec = DatasetSpec.parse('http://localhost:8000/foo:ds');
    invariant(spec);
    assert.equal(spec.name, 'ds');
    assert.equal(spec.database.scheme, 'http');
    assert.equal(spec.database.path, '//localhost:8000/foo');
    ds = spec.dataset();
    assert.instanceOf(ds, Dataset);
    assert.instanceOf(ds.database._vs._bs, HttpBatchStore);
    await ds.database.close();
  });

  test('HashSpec', async () => {
    const testHash = new Hash('sha1-0000000000000000000000000000000000000000');
    const invalid = [
      'mem', 'mem:', 'http', 'http:', 'http://foo', 'monkey', 'monkey:balls',
      'mem:not-hash', 'mem:sha1-', 'mem:sha2-0000',
      'http://foo:blah', 'https://foo:sha1',
    ];
    invalid.forEach(s => assert.isNull(HashSpec.parse(s)));

    const spec = HashSpec.parse(`mem:${testHash}`);
    invariant(spec);
    assert.equal(spec.hash.toString(), testHash.toString());
    assert.equal(spec.database.scheme, 'mem');
    assert.equal(spec.database.path, '');
  });

  test('ObjectSpec', () => {
    let spec = parseObjectSpec('http://foo:8000/test:monkey');
    invariant(spec);
    assert.isNotNull(spec.value());
    invariant(spec instanceof DatasetSpec);
    assert.equal(spec.name, 'monkey');
    assert.equal(spec.database.scheme, 'http');
    assert.equal(spec.database.path, '//foo:8000/test');

    const testHash = new Hash('sha1-0000000000000000000000000000000000000000');
    spec = parseObjectSpec(`http://foo:8000/test:${testHash}`);
    invariant(spec);
    assert.isNotNull(spec.value());
    invariant(spec instanceof HashSpec);
    assert.equal(spec.hash.toString(), testHash.toString());
  });
});
