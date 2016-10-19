// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import Hash from './hash.js';
import {assert} from 'chai';
import {ensureHash} from './get-hash.js';
import {suite, test} from 'mocha';

suite('get hash', () => {
  test('ensureHash', () => {
    let h: ?Hash = null;
    h = ensureHash(h, false);
    assert.isNotNull(h);
    assert.strictEqual(h, ensureHash(h, false));
  });
});
