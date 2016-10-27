// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';
import {invariant} from './assert.js';
import {getHash} from './get-hash.js';
import List from './list.js';
import {DatabaseSpec, DatasetSpec, PathSpec} from './specs.js';

const assertThrowsSyntaxError = (parse, s) => {
  let msg = '';
  try {
    parse(s);
  } catch (e) {
    assert.instanceOf(e, SyntaxError,
                      `${s} did not produce a SyntaxError, instead ${e.constructor.name}`);
    msg = e.message;
  }
  assert.notEqual('', msg, `${s} did not produce an error`);
};

suite('Specs', () => {
  test('mem database', async () => {
    const spec = DatabaseSpec.parse('mem');
    assert.strictEqual('mem', spec.scheme);
    assert.strictEqual('', spec.path);

    const db = spec.database();
    db.writeValue(true);
    assert.strictEqual(true, await spec.database().readValue(getHash(true)));
  });

  test('mem dataset', async () => {
    const spec = DatasetSpec.parse('mem::test');
    assert.strictEqual('test', spec.name);

    let [, head] = await spec.value();
    assert.strictEqual(null, head);

    const [db, ds] = await spec.dataset();
    await db.commit(ds, 'Commit Value');
    [, head] = await spec.value();
    assert.strictEqual('Commit Value', head);
  });

  test('mem hash path', async () => {
    const trueHash = getHash(true).toString();
    const spec = PathSpec.parse(`mem::#${trueHash}`);

    let [db, value] = await spec.value();
    assert.strictEqual(null, value);

    db.writeValue(true);
    [db, value] = await spec.value();
    assert.strictEqual(true, value);
  });

  test('mem dataset path', async () => {
    const spec = PathSpec.parse('mem::test.value[0]');

    let [db, value] = await spec.value();
    assert.strictEqual(null, value);

    const ds = await db.getDataset('test');
    await db.commit(ds, new List([42]));
    [db, value] = await spec.value();
    assert.strictEqual(42, value);
  });

  test('DatabaseSpec', async () => {
    const invalid = [
      'mem:stuff', 'mem::', 'mem:', 'http:', 'https:', 'random:', 'random:random',
      'local', './local', 'ldb', 'ldb:', 'ldb:local',
    ];
    invalid.forEach(s => assertThrowsSyntaxError(DatabaseSpec.parse, s));

    const valid = [
      {spec: 'http://localhost:8000', scheme: 'http', path: '//localhost:8000'},
      {spec: 'http://localhost:8000/fff', scheme: 'http', path: '//localhost:8000/fff'},
      {spec: 'https://local.attic.io/john/doe', scheme: 'https', path: '//local.attic.io/john/doe'},
      {spec: 'mem', scheme: 'mem', path: ''},
      {spec: 'http://server.com/john/doe?access_token=jane', scheme: 'http',
        path: '//server.com/john/doe?access_token=jane'},
      {spec: 'https://server.com/john/doe/?arg=2&qp1=true&access_token=jane', scheme: 'https',
        path: '//server.com/john/doe/?arg=2&qp1=true&access_token=jane'},
      // TODO: This isn't valid, see https://github.com/attic-labs/noms/issues/2351.
      {spec: 'http://some/::/one', scheme: 'http', path: '//some/::/one'},
      {spec: 'http://::1', scheme: 'http', path: '//::1'},
      {spec: 'http://192.30.252.154', scheme: 'http', path: '//192.30.252.154'},
      {spec: 'http://::192.30.252.154', scheme: 'http', path: '//::192.30.252.154'},
      {spec: 'http://0:0:0:0:0:ffff:c01e:fc9a', scheme: 'http', path: '//0:0:0:0:0:ffff:c01e:fc9a'},
      {spec: 'http://::ffff:c01e:fc9a', scheme: 'http', path: '//::ffff:c01e:fc9a'},
      {spec: 'http://::ffff::1e::9a', scheme: 'http', path: '//::ffff::1e::9a'},
    ];

    for (const tc of valid) {
      const spec = DatabaseSpec.parse(tc.spec);
      assert.strictEqual(spec.scheme, tc.scheme);
      assert.strictEqual(spec.path, tc.path);
      assert.strictEqual(tc.spec, spec.toString());
    }
  });

  test('DatasetSpec', async () => {
    const assertInvalid = s => assertThrowsSyntaxError(DatasetSpec.parse, s);

    const invalid = [
      'mem', 'mem:', 'http', 'http:', 'http://foo', 'monkey', 'monkey:balls',
      'http::dsname', 'http:::dsname', 'mem:/a/bogus/path::dsname',
      'ldb:', 'ldb:hello',
    ];
    invalid.forEach(assertInvalid);

    const invalidDatasetNames = [' ', '', '$', '#', ':', '\n', '💩'];
    invalidDatasetNames.map(s => `mem::${s}`).forEach(assertInvalid);

    const validDatasetNames = ['a', 'Z', '0','/', '-', '_'];
    for (const s of validDatasetNames) {
      DatasetSpec.parse(`mem::${s}`);
    }

    const valid = [
      {spec: 'http://localhost:8000/foo::ds', scheme: 'http', path: '//localhost:8000/foo',
        name: 'ds'},
      {spec: 'http://localhost:8000::ds1', scheme: 'http', path: '//localhost:8000', name: 'ds1'},
      {spec: 'http://localhost:8000/john/doe/::ds2', scheme: 'http',
        path: '//localhost:8000/john/doe/', name: 'ds2'},
      {spec: 'https://local.attic.io/john/doe::ds3', scheme: 'https',
        path: '//local.attic.io/john/doe', name: 'ds3'},
      {spec: 'http://local.attic.io/john/doe::ds1', scheme: 'http',
        path: '//local.attic.io/john/doe', name: 'ds1'},
      {spec: 'http://localhost:8000/john/doe?access_token=abc::ds/one', scheme: 'http',
        path: '//localhost:8000/john/doe?access_token=abc', name: 'ds/one'},
      {spec: 'https://localhost:8000?qp1=x&access_token=abc&qp2=y::ds/one', scheme: 'https',
        path: '//localhost:8000?qp1=x&access_token=abc&qp2=y', name: 'ds/one'},
      {spec: 'http://localhost:8000/pa::th/foo::ds', scheme: 'http',
        path: '//localhost:8000/pa::th/foo', name: 'ds'},
      {spec: 'http://192.30.252.154::foo', scheme: 'http', path: '//192.30.252.154', name: 'foo'},
      {spec: 'http://::1::foo', scheme: 'http', path: '//::1', name: 'foo'},
      {spec: 'http://::192.30.252.154::foo', scheme: 'http', path: '//::192.30.252.154',
        name: 'foo'},
      {spec: 'http://0:0:0:0:0:ffff:c01e:fc9a::foo', scheme: 'http',
        path: '//0:0:0:0:0:ffff:c01e:fc9a', name: 'foo'},
      {spec: 'http://::ffff:c01e:fc9a::foo', scheme: 'http', path: '//::ffff:c01e:fc9a',
        name: 'foo'},
      {spec: 'http://::ffff::1e::9a::foo', scheme: 'http', path: '//::ffff::1e::9a', name: 'foo'},
    ];

    for (const tc of valid) {
      const spec = DatasetSpec.parse(tc.spec);
      const {scheme, path} = spec.database;
      assert.strictEqual(tc.scheme, scheme);
      assert.strictEqual(tc.path, path);
      assert.strictEqual(tc.name, spec.name);
      assert.strictEqual(tc.spec, spec.toString());
    }
  });

  test('PathSpec', async () => {
    const badSpecs = ['mem::#', 'mem::#s', 'mem::#foobarbaz', 'mem::.hello', 'ldb:path::foo.bar'];
    badSpecs.forEach(bs => assertThrowsSyntaxError(PathSpec.parse, bs));

    const valid = [
      {spec: 'http://local.attic.io/john/doe::#0123456789abcdefghijklmnopqrstuv', scheme: 'http',
        dbPath: '//local.attic.io/john/doe', pathStr: '#0123456789abcdefghijklmnopqrstuv'},
      {spec: 'mem::#0123456789abcdefghijklmnopqrstuv', scheme: 'mem', dbPath: '',
        pathStr: '#0123456789abcdefghijklmnopqrstuv'},
      {spec: 'http://local.attic.io/john/doe::#0123456789abcdefghijklmnopqrstuv', scheme: 'http',
        dbPath: '//local.attic.io/john/doe', pathStr: '#0123456789abcdefghijklmnopqrstuv'},
      {spec: 'http://localhost:8000/john/doe/::ds1', scheme: 'http',
        dbPath: '//localhost:8000/john/doe/', pathStr: 'ds1'},
      {spec: 'http://192.30.252.154::foo.bar', scheme: 'http', dbPath: '//192.30.252.154',
        pathStr: 'foo.bar'},
      {spec: 'http://::1::foo.bar.baz', scheme: 'http', dbPath: '//::1', pathStr: 'foo.bar.baz'},
      {spec: 'http://::192.30.252.154::baz[42]', scheme: 'http', dbPath: '//::192.30.252.154',
        pathStr: 'baz[42]'},
      {spec: 'http://0:0:0:0:0:ffff:c01e:fc9a::foo[42].bar', scheme: 'http',
        dbPath: '//0:0:0:0:0:ffff:c01e:fc9a', pathStr: 'foo[42].bar'},
      {spec: 'http://::ffff:c01e:fc9a::foo.foo', scheme: 'http', dbPath: '//::ffff:c01e:fc9a',
        pathStr: 'foo.foo'},
      {spec: 'http://::ffff::1e::9a::hello["world"]', scheme: 'http', dbPath: '//::ffff::1e::9a',
        pathStr: 'hello["world"]'},
    ];

    for (const tc of valid) {
      const spec = PathSpec.parse(tc.spec);
      const {scheme, path} = spec.database;
      assert.strictEqual(tc.scheme, scheme);
      assert.strictEqual(tc.dbPath, path);
      assert.strictEqual(tc.pathStr, spec.path.toString());
      assert.strictEqual(tc.spec, spec.toString());
    }
  });


  test('PathSpec.pin', async () => {
    const dbSpec = DatabaseSpec.parse('mem');
    const db = dbSpec.database();

    let ds = db.getDataset('foo');
    ds = await db.commit(ds, 42);

    const unpinned = PathSpec.parse('mem::foo.value');
    unpinned.database = dbSpec;

    const pinned = await unpinned.pin();
    invariant(pinned);
    const pinnedHash = pinned.path.hash;
    invariant(pinnedHash);
    const h = await ds.head();
    invariant(h);
    assert.strictEqual(h.hash.toString(), pinnedHash.toString());
    assert.strictEqual(`mem::#${h.hash.toString()}.value`, pinned.toString());
    assert.strictEqual(42, (await pinned.value())[1]);
    assert.strictEqual(42, (await unpinned.value())[1]);

    ds = await db.commit(ds, 43);
    assert.strictEqual(42, (await pinned.value())[1]);
    assert.strictEqual(43, (await unpinned.value())[1]);

    const pinned1 = PathSpec.parse('mem::#imgp9mp1h3b9nv0gna6mri53dlj9f4ql.value');
    assert.strictEqual(pinned1, await pinned1.pin());
  });
});
