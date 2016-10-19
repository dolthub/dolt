// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import HTTPError from './http-error.js';
import {assert} from 'chai';
import {suite, test} from 'mocha';

suite('http-error', () => {
  test('prototype', () => {
    assert.equal(HTTPError.prototype.__proto__, Error.prototype);

    const e = new HTTPError(42);
    assert.equal(e.__proto__, HTTPError.prototype);
  });

  test('instanceof', () => {
    const e = new HTTPError(42);
    assert.isTrue(e instanceof HTTPError);
    assert.isTrue(e instanceof Error);
  });

  test('message', () => {
    const e = new HTTPError(42);
    assert.strictEqual(e.message, '42');
  });

  test('name', () => {
    const e = new HTTPError(42);
    assert.strictEqual(e.name, 'HTTPError');
  });

  test('toString', () => {
    const e = new HTTPError(42);
    assert.strictEqual(e.toString(), 'HTTPError: 42');
  });
});
