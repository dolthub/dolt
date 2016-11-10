// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import HttpError from './http-error.js';
import {assert} from 'chai';
import {suite, test} from 'mocha';

suite('http-error', () => {
  test('prototype', () => {
    assert.equal(HttpError.prototype.__proto__, Error.prototype);

    const e = new HttpError(42);
    assert.equal(e.__proto__, HttpError.prototype);
  });

  test('instanceof', () => {
    const e = new HttpError(42);
    assert.isTrue(e instanceof HttpError);
    assert.isTrue(e instanceof Error);
  });

  test('message', () => {
    const e = new HttpError(42);
    assert.strictEqual(e.message, '42');
  });

  test('name', () => {
    const e = new HttpError(42);
    assert.strictEqual(e.name, 'HttpError');
  });

  test('toString', () => {
    const e = new HttpError(42);
    assert.strictEqual(e.toString(), 'HttpError: 42');
  });
});
