// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';
import {invariant} from './assert.js';
import {getHash} from './get-hash.js';
import List from './list.js';
import Spec from './spec.js';
import Struct, {StructMirror} from './struct.js';
import type Value from './value.js';

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

suite('Spec', () => {
  test('mem database', async () => {
    const spec = Spec.forDatabase('mem');
    assert.strictEqual('mem', spec.protocol());
    assert.strictEqual('', spec.databaseName());

    spec.database().writeValue(true);
    assert.strictEqual(true, await spec.database().readValue(getHash(true)));
  });

  test('mem dataset', async () => {
    const spec = Spec.forDataset('mem::test');
    assert.strictEqual('mem', spec.protocol());
    assert.strictEqual('', spec.databaseName());
    assert.strictEqual('test', spec.datasetName());

    let head = await spec.dataset().headValue();
    assert.strictEqual(null, head);

    await spec.database().commit(spec.dataset(), 'Commit Value');
    head = await spec.dataset().headValue();
    assert.strictEqual('Commit Value', head);
  });

  test('mem hash path', async () => {
    const trueHash = getHash(true).toString();
    const spec = Spec.forPath(`mem::#${trueHash}`);

    assert.strictEqual(null, await spec.value());

    spec.database().writeValue(true);
    assert.strictEqual(true, await spec.value());
  });

  test('mem dataset path', async () => {
    const spec = Spec.forPath('mem::test.value[0]');

    assert.strictEqual(null, await spec.value());

    const db = spec.database();
    await db.commit(db.getDataset('test'), new List([42]));
    assert.strictEqual(42, await spec.value());
  });

  test('database spec', async () => {
    const invalid = [
      'mem:stuff',
      'mem:',
      'mem::',
      'http:',
      'https:',
      // See issue https://github.com/attic-labs/noms/issues/2351:
      // 'http://',
      // 'http://%',
      // 'https://',
      // 'https://%',
      'random:',
      'random:random',
      '/file/ba:d',
      'local',
      './local',
      'ldb',
      'ldb:',
      'ldb:local',
    ];
    invalid.forEach(s => assertThrowsSyntaxError(Spec.forDatabase, s));

    const valid = [
      {spec: 'http://localhost:8000', protocol: 'http', databaseName: 'localhost:8000'},
      {spec: 'http://localhost:8000/fff', protocol: 'http', databaseName: 'localhost:8000/fff'},
      {spec: 'https://local.attic.io/john/doe', protocol: 'https',
        databaseName: 'local.attic.io/john/doe'},
      {spec: 'mem', protocol: 'mem', databaseName: ''},
      {spec: 'http://server.com/john/doe?access_token=jane', protocol: 'http',
        databaseName: 'server.com/john/doe?access_token=jane'},
      {spec: 'https://server.com/john/doe/?arg=2&qp1=true&access_token=jane', protocol: 'https',
        databaseName: 'server.com/john/doe/?arg=2&qp1=true&access_token=jane'},
      {spec: 'http://some/::/one', protocol: 'http', databaseName: 'some/::/one'},
      {spec: 'http://::1', protocol: 'http', databaseName: '::1'},
      {spec: 'http://192.30.252.154', protocol: 'http', databaseName: '192.30.252.154'},
      {spec: 'http://::192.30.252.154', protocol: 'http', databaseName: '::192.30.252.154'},
      {spec: 'http://0:0:0:0:0:ffff:c01e:fc9a', protocol: 'http',
        databaseName: '0:0:0:0:0:ffff:c01e:fc9a'},
      {spec: 'http://::ffff:c01e:fc9a', protocol: 'http', databaseName: '::ffff:c01e:fc9a'},
      {spec: 'http://::ffff::1e::9a', protocol: 'http', databaseName: '::ffff::1e::9a'},
    ];

    for (const tc of valid) {
      const spec = Spec.forDatabase(tc.spec);
      assert.strictEqual(tc.protocol, spec.protocol());
      assert.strictEqual(tc.databaseName, spec.databaseName());
      assert.strictEqual(tc.spec, spec.spec());
    }
  });

  test('dataset spec', async () => {
    const assertInvalid = s => assertThrowsSyntaxError(Spec.forDataset, s);

    const invalid = [
      'mem',
      'mem:',
      'http',
      'http:',
      'http://foo',
      'monkey',
      'monkey:balls',
      'http::dsname',
      'http:::dsname',
      'mem:/a/bogus/path::dsname',
      'ldb:',
      'ldb:hello',
    ];
    invalid.forEach(assertInvalid);

    const invalidDatasetNames = [' ', '', '$', '#', ':', '\n', '💩'];
    invalidDatasetNames.map(s => `mem::${s}`).forEach(assertInvalid);

    const validDatasetNames = ['a', 'Z', '0','/', '-', '_'];
    for (const s of validDatasetNames) {
      Spec.forDataset(`mem::${s}`);
    }

    const valid = [
      {spec: 'http://localhost:8000/foo::ds', protocol: 'http', databaseName: 'localhost:8000/foo',
        datasetName: 'ds'},
      {spec: 'http://localhost:8000::ds1', protocol: 'http', databaseName: 'localhost:8000',
        datasetName: 'ds1'},
      {spec: 'http://localhost:8000/john/doe/::ds2', protocol: 'http',
        databaseName: 'localhost:8000/john/doe/', datasetName: 'ds2'},
      {spec: 'https://local.attic.io/john/doe::ds3', protocol: 'https',
        databaseName: 'local.attic.io/john/doe', datasetName: 'ds3'},
      {spec: 'http://local.attic.io/john/doe::ds1', protocol: 'http',
        databaseName: 'local.attic.io/john/doe', datasetName: 'ds1'},
      {spec: 'http://localhost:8000/john/doe?access_token=abc::ds/one', protocol: 'http',
        databaseName: 'localhost:8000/john/doe?access_token=abc', datasetName: 'ds/one'},
      {spec: 'https://localhost:8000?qp1=x&access_token=abc&qp2=y::ds/one', protocol: 'https',
        databaseName: 'localhost:8000?qp1=x&access_token=abc&qp2=y', datasetName: 'ds/one'},
      {spec: 'http://localhost:8000/pa::th/foo::ds', protocol: 'http',
        databaseName: 'localhost:8000/pa::th/foo', datasetName: 'ds'},
      {spec: 'http://192.30.252.154::foo', protocol: 'http', databaseName: '192.30.252.154',
        datasetName: 'foo'},
      {spec: 'http://::1::foo', protocol: 'http', databaseName: '::1', datasetName: 'foo'},
      {spec: 'http://::192.30.252.154::foo', protocol: 'http', databaseName: '::192.30.252.154',
        datasetName: 'foo'},
      {spec: 'http://0:0:0:0:0:ffff:c01e:fc9a::foo', protocol: 'http',
        databaseName: '0:0:0:0:0:ffff:c01e:fc9a', datasetName: 'foo'},
      {spec: 'http://::ffff:c01e:fc9a::foo', protocol: 'http', databaseName: '::ffff:c01e:fc9a',
        datasetName: 'foo'},
      {spec: 'http://::ffff::1e::9a::foo', protocol: 'http', databaseName: '::ffff::1e::9a',
        datasetName: 'foo'},
    ];

    for (const tc of valid) {
      const spec = Spec.forDataset(tc.spec);
      assert.strictEqual(tc.protocol, spec.protocol());
      assert.strictEqual(tc.databaseName, spec.databaseName());
      assert.strictEqual(tc.datasetName, spec.datasetName());
      assert.strictEqual(tc.spec, spec.spec());
    }
  });

  test('path spec', async () => {
    const badSpecs = [
      'mem::#',
      'mem::#s',
      'mem::#foobarbaz',
      'mem::.hello',
      'ldb:path::foo.bar',
    ];
    badSpecs.forEach(bs => assertThrowsSyntaxError(Spec.forPath, bs));

    const valid = [
      {spec: 'http://local.attic.io/john/doe::#0123456789abcdefghijklmnopqrstuv', protocol: 'http',
        databaseName: 'local.attic.io/john/doe', path: '#0123456789abcdefghijklmnopqrstuv'},
      {spec: 'mem::#0123456789abcdefghijklmnopqrstuv', protocol: 'mem', databaseName: '',
        path: '#0123456789abcdefghijklmnopqrstuv'},
      {spec: 'http://local.attic.io/john/doe::#0123456789abcdefghijklmnopqrstuv', protocol: 'http',
        databaseName: 'local.attic.io/john/doe', path: '#0123456789abcdefghijklmnopqrstuv'},
      {spec: 'http://localhost:8000/john/doe/::ds1', protocol: 'http',
        databaseName: 'localhost:8000/john/doe/', path: 'ds1'},
      {spec: 'http://192.30.252.154::foo.bar', protocol: 'http', databaseName: '192.30.252.154',
        path: 'foo.bar'},
      {spec: 'http://::1::foo.bar.baz', protocol: 'http', databaseName: '::1', path: 'foo.bar.baz'},
      {spec: 'http://::192.30.252.154::baz[42]', protocol: 'http', databaseName: '::192.30.252.154',
        path: 'baz[42]'},
      {spec: 'http://0:0:0:0:0:ffff:c01e:fc9a::foo[42].bar', protocol: 'http',
        databaseName: '0:0:0:0:0:ffff:c01e:fc9a', path: 'foo[42].bar'},
      {spec: 'http://::ffff:c01e:fc9a::foo.foo', protocol: 'http', databaseName: '::ffff:c01e:fc9a',
        path: 'foo.foo'},
      {spec: 'http://::ffff::1e::9a::hello["world"]', protocol: 'http',
        databaseName: '::ffff::1e::9a', path: 'hello["world"]'},
    ];

    for (const tc of valid) {
      const spec = Spec.forPath(tc.spec);
      assert.strictEqual(tc.protocol, spec.protocol());
      assert.strictEqual(tc.databaseName, spec.databaseName());
      assert.strictEqual(tc.path, spec.path().toString());
      assert.strictEqual(tc.spec, spec.spec());
    }
  });

  test('pin path spec', async () => {
    const unpinned = Spec.forPath('mem::foo.value');

    const db = unpinned.database();
    await db.commit(db.getDataset('foo'), 42);

    const pinned = await unpinned.pin();
    invariant(pinned);

    const pinnedHash = pinned.path().hash;
    invariant(pinnedHash);

    const h = await db.getDataset('foo').head();
    invariant(h);

    assert.strictEqual(h.hash.toString(), pinnedHash.toString());
    assert.strictEqual(`mem::#${h.hash.toString()}.value`, pinned.spec());
    assert.strictEqual(42, await pinned.value());
    assert.strictEqual(42, await unpinned.value());

    await db.commit(db.getDataset('foo'), 43);
    assert.strictEqual(42, await pinned.value());
    assert.strictEqual(43, await unpinned.value());
  });

  test('pin dataset spec', async () => {
    const unpinned = Spec.forDataset('mem::foo');

    const db = unpinned.database();
    await db.commit(db.getDataset('foo'), 42);

    const pinned = await unpinned.pin();
    invariant(pinned);

    const pinnedHash = pinned.path().hash;
    invariant(pinnedHash);

    const h = await db.getDataset('foo').head();
    invariant(h);

    const commitValue = (commit: ?Value) => {
      invariant(commit instanceof Struct);
      return new StructMirror(commit).get('value');
    };

    assert.strictEqual(h.hash.toString(), pinnedHash.toString());
    assert.strictEqual(`mem::#${h.hash.toString()}`, pinned.spec());
    assert.strictEqual(42, commitValue(await pinned.value()));
    assert.strictEqual(42, await unpinned.dataset().headValue());

    await db.commit(db.getDataset('foo'), 43);
    assert.strictEqual(42, commitValue(await pinned.value()));
    assert.strictEqual(43, await unpinned.dataset().headValue());
  });

  test('already pinned', async () => {
    const spec = Spec.forPath('mem::#imgp9mp1h3b9nv0gna6mri53dlj9f4ql.value');
    assert.strictEqual(spec, await spec.pin());
  });
});
