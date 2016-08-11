// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {invariant} from './assert.js';
import Database from './database.js';
import Hash from './hash.js';
import {DatabaseSpec, DatasetSpec, HashSpec, parseObjectSpec} from './specs.js';
import {assert} from 'chai';
import {suite, test} from 'mocha';

suite('Specs', () => {
  test('DatabaseSpec', async () => {
    const invalid = ['mem:stuff', 'mem::', 'mem:', 'http:', 'https:', 'random:', 'random:random',
      'http://some/::/one',
    ];
    invalid.forEach(s => assert.isNull(DatabaseSpec.parse(s)));

    const valid = [
      {spec: 'mem', scheme: 'mem', path: ''},
      {spec: 'https://foo/path', scheme: 'https', path: '//foo/path'},
    ];

    valid.forEach(async (tc) => {
      const spec = DatabaseSpec.parse(tc.spec);
      invariant(spec);
      assert.equal(spec.scheme, tc.scheme);
      assert.equal(spec.path, tc.path);
      const database = spec.database();
      assert.instanceOf(database, Database);
      await database.close();
    });
  });

  test('DatasetSpec', async () => {
    const invalid = ['mem', 'mem:', 'http', 'http:', 'http://foo', 'monkey', 'monkey:balls',
      'http::dsname', 'http:::dsname', 'mem:/a/bogus/path::dsname',
      'http://localhost:8000/pa::th/foo::ds',
    ];
    invalid.forEach(s => assert.isNull(DatasetSpec.parse(s)));

    const invalidDatasetNames = [' ', '', '$', '#', ':', '\n', '💩'];
    for (const s of invalidDatasetNames) {
      assert.isNull(DatasetSpec.parse(`mem::${s}`));
    }

    const validDatasetNames = ['a', 'Z', '0','/', '-', '_'];
    for (const s of validDatasetNames) {
      assert.isNotNull(DatasetSpec.parse(`mem::${s}`));
    }

    const valid = [
      {spec: 'mem::ds', scheme: 'mem', path: '', name: 'ds'},
      {spec: 'mem:::ds', scheme: 'mem', path: '', name: 'ds'},
      {spec: 'http://localhost:8000/foo::ds', scheme: 'http', path: '//localhost:8000/foo',
        name: 'ds'},
    ];

    valid.forEach(async (tc) => {
      const spec = DatasetSpec.parse(tc.spec);
      invariant(spec);
      assert.equal(spec.database.scheme, tc.scheme);
      assert.equal(spec.database.path, tc.path);
      assert.equal(spec.name, 'ds');
      const database = spec.database.database();
      assert.instanceOf(database, Database);
      await database.close();
    });
  });

  test('HashSpec', async () => {
    const testHash = Hash.parse('00000000000000000000000000000000');
    invariant(testHash);
    const invalid = [
      'mem', 'mem:', 'http', 'http:', 'http://foo', 'monkey', 'monkey:balls',
      'mem:not-hash', 'mem:0000', `mem:::${testHash.toString()}`,
      'http://foo:blah',
    ];
    invalid.forEach(s => assert.isNull(HashSpec.parse(s)));

    const valid = [
      {spec: `mem::${testHash.toString()}`, protocol: 'mem', path: '', hash: testHash.toString()},
      {spec: `http://someserver.com/some/path::${testHash.toString()}`,
        protocol: 'http', path: '//someserver.com/some/path', hash: testHash.toString()},
    ];
    valid.forEach(tc => {
      const spec = HashSpec.parse(tc.spec);
      invariant(spec, `${tc.spec} failed to parse`);
      assert.equal(spec.hash.toString(), tc.hash);
      assert.equal(spec.database.scheme, tc.protocol);
      assert.equal(spec.database.path, tc.path);
    });
  });

  test('ObjectSpec', () => {
    let spec = parseObjectSpec('http://foo:8000/test::monkey');
    invariant(spec);
    assert.isNotNull(spec.value());
    invariant(spec instanceof DatasetSpec);
    assert.equal(spec.name, 'monkey');
    assert.equal(spec.database.scheme, 'http');
    assert.equal(spec.database.path, '//foo:8000/test');

    const testHash = Hash.parse('00000000000000000000000000000000');
    invariant(testHash);
    spec = parseObjectSpec(`http://foo:8000/test::${testHash.toString()}`);
    invariant(spec);
    assert.isNotNull(spec.value());
    invariant(spec instanceof HashSpec);
    assert.equal(spec.hash.toString(), testHash.toString());
  });
});
