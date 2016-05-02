// @flow

import {suite, test} from 'mocha';
import Chunk from './chunk.js';
import {assert} from 'chai';
import {Delegate} from './http-batch-store.js';
import {deserialize} from './chunk-serializer.js';

suite('HttpBatchStore', () => {
  test('build write request', async () => {
    const canned = new Set([Chunk.fromString('abc').ref, Chunk.fromString('def').ref]);
    const reqs = [
      {c: Chunk.fromString('ghi'), hints: new Set()},
      {c: Chunk.fromString('wacka wack wack'), hints: canned},
    ];
    const d = new Delegate(
      {getRefs: '', writeValue: '', root: ''},
      {method: 'POST', headers: {}});
    const body = d._buildWriteRequest(reqs);
    const {hints, chunks} = deserialize(body);
    assert.equal(2, chunks.length);
    assert.equal(2, hints.length);
  });
});