// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import jest, {suite, test, setup, teardown} from './jest.js';
import {assert} from 'chai';
import Hash from './hash.js';
import {emptyHash} from './hash.js';
import HttpBatchStore from './http-batch-store.js';
import {invariant} from './assert.js';


suite('HttpBatchStore', () => {
  setup(() => {
    jest.mock('http', () => ({
      request(options, cb) {
        cb({statusCode: 409});
        return {
          end() {},
          on() {},
          setTimeout() {},
        };
      },
    }));
  });

  teardown(() => {
    jest.resetModules();
  });

  test('endpoints', async () => {
    const getRefsEndpoint = '/getRefs/';
    const rootEndpoint = '/root/';
    const writeValueEndpoint = '/writeValue/';

    const vals = [
      {host: 'http://localhost:8000', params: '?access_token=test1'},
      {host: 'http://demo.noms.io/one/two', params: '?extra=something&access_token=test1'},
      {host: 'http://localhost:8001', params: ''},
      {host: 'http://demo.noms.io/one/two', params: ''},
    ];

    for (const {host, params} of vals) {
      const store = new HttpBatchStore(host + params);
      const rpc = store._rpc;

      assert.equal(host + getRefsEndpoint + params, rpc.getRefs);
      assert.equal(host + rootEndpoint + params, rpc.root);
      assert.equal(host + writeValueEndpoint + params, rpc.writeValue);

      const store1 = new HttpBatchStore(host + '/' + params);
      const rpc1 = store1._rpc;
      assert.equal(host + getRefsEndpoint + params, rpc1.getRefs);
      assert.equal(host + rootEndpoint + params, rpc1.root);
      assert.equal(host + writeValueEndpoint + params, rpc1.writeValue);
    }
  });

  test('updateRoot conflict', async () => {
    const HttpBatchStore = require('./http-batch-store.js').default;
    const store = new HttpBatchStore('http://nowhere.com');

    const h = Hash.parse('00001111000011110000111100001111');
    invariant(h);
    assert.isFalse(await store.updateRoot(h, emptyHash));
  });
});
