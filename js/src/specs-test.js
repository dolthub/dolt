// @flow

import {invariant} from './assert.js';
import Dataset from './dataset.js';
import Database from './database.js';
import HttpStore from './http-store.js';
import MemoryStore from './memory-store.js';
import Ref from './ref.js';
import {DatabaseSpec, DatasetSpec, RefSpec, parseObjectSpec} from './specs.js';
import {assert} from 'chai';
import {suite, test} from 'mocha';

suite('Specs', () => {
  test('DatabaseSpec', () => {
    const notAllowed = ['mem:', 'mem:stuff', 'http:', 'https:', 'random:', 'random:random'];
    notAllowed.forEach(s => assert.isNull(DatabaseSpec.parse(s)));

    let spec = DatabaseSpec.parse('mem');
    invariant(spec);
    assert.equal(spec.scheme, 'mem');
    assert.equal(spec.path, '');
    assert.instanceOf(spec.db(), Database);
    assert.instanceOf(spec.db()._cs, MemoryStore);

    spec = DatabaseSpec.parse('http://foo');
    invariant(spec);
    assert.isNotNull(spec);
    assert.equal(spec.scheme, 'http');
    assert.equal(spec.path, '//foo');
    assert.instanceOf(spec.db(), Database);
    assert.instanceOf(spec.db()._cs, HttpStore);

    spec = DatabaseSpec.parse('https://foo');
    invariant(spec);
    assert.isNotNull(spec);
    assert.equal(spec.scheme, 'https');
    assert.equal(spec.path, '//foo');
  });

  test('DataSetSpec', () => {
    const invalid = ['mem', 'mem:', 'http', 'http:', 'http://foo', 'monkey', 'monkey:balls'];
    invalid.forEach(s => assert.isNull(DatasetSpec.parse(s)));

    let spec = DatasetSpec.parse('mem:ds');
    invariant(spec);
    assert.equal(spec.name, 'ds');
    assert.equal(spec.db.scheme, 'mem');
    assert.equal(spec.db.path, '');
    let ds = spec.set();
    assert.instanceOf(ds, Dataset);
    assert.instanceOf(ds.db._cs, MemoryStore);

    spec = DatasetSpec.parse('http://localhost:8000/foo:ds');
    invariant(spec);
    assert.equal(spec.name, 'ds');
    assert.equal(spec.db.scheme, 'http');
    assert.equal(spec.db.path, '//localhost:8000/foo');
    ds = spec.set();
    assert.instanceOf(ds, Dataset);
    assert.instanceOf(ds.db._cs, HttpStore);
  });

  test('RefSpec', () => {
    const testRef = new Ref('sha1-0000000000000000000000000000000000000000');
    const invalid = [
      'mem', 'mem:', 'http', 'http:', 'http://foo', 'monkey', 'monkey:balls',
      'mem:not-ref', 'mem:sha1-', 'mem:sha2-0000',
      'http://foo:blah', 'https://foo:sha1',
    ];
    invalid.forEach(s => assert.isNull(RefSpec.parse(s)));

    const spec = RefSpec.parse(`mem:${testRef}`);
    invariant(spec);
    assert.equal(spec.ref.toString(), testRef.toString());
    assert.equal(spec.db.scheme, 'mem');
    assert.equal(spec.db.path, '');
  });

  test('ObjectSpec', () => {
    let spec = parseObjectSpec('http://foo:8000/test:monkey');
    invariant(spec);
    assert.isNotNull(spec.value());
    invariant(spec instanceof DatasetSpec);
    assert.equal(spec.name, 'monkey');
    assert.equal(spec.db.scheme, 'http');
    assert.equal(spec.db.path, '//foo:8000/test');

    const testRef = new Ref('sha1-0000000000000000000000000000000000000000');
    spec = parseObjectSpec(`http://foo:8000/test:${testRef}`);
    invariant(spec);
    assert.isNotNull(spec.value());
    invariant(spec instanceof RefSpec);
    assert.equal(spec.ref.toString(), testRef.toString());
  });
});
