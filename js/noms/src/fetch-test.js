// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {assert} from 'chai';
import {suite, test, setup, teardown} from 'mocha';
import mock from 'mock-require';

suite('fetch', () => {
  let log;

  setup(() => {
    log = [];
    for (const protocol of ['http', 'https']) {
      mock(protocol, {
        request(options) {
          log.push(protocol, options.href);
          return {
            end() {},
            on() {},
            setTimeout() {},
          };
        },
      });
    }
  });

  teardown(() => {
    mock.stopAll();
  });

  test('http vs https', () => {
    const {fetchText} = mock.reRequire('./fetch.js');

    fetchText('http://example.com');
    assert.deepEqual(log, ['http', 'http://example.com/']);

    log = [];
    fetchText('https://example.com');
    assert.deepEqual(log, ['https', 'https://example.com/']);
  });
});
