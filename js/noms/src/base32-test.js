// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {encode, decode} from './base32.js';
import {assert} from 'chai';
import {alloc} from './bytes.js';
import {suite, test} from 'mocha';

suite('base32', () => {
  test('encode', () => {
    const a = alloc(20);
    assert.equal(encode(a).length, 32);
    assert.equal(encode(a), '00000000000000000000000000000000');
    a[a.length - 1] = 1;
    assert.equal(encode(a), '00000000000000000000000000000001');
    a[a.length - 1] = 10;
    assert.equal(encode(a), '0000000000000000000000000000000a');
    a[a.length - 1] = 20;
    assert.equal(encode(a), '0000000000000000000000000000000k');
    a[a.length - 1] = 31;
    assert.equal(encode(a), '0000000000000000000000000000000v');
    a[a.length - 1] = 32;
    assert.equal(encode(a), '00000000000000000000000000000010');
    a[a.length - 1] = 63;
    assert.equal(encode(a), '0000000000000000000000000000001v');
    a[a.length - 1] = 64;
    assert.equal(encode(a), '00000000000000000000000000000020');

    // Largest!
    for (let i = 0; i < a.length; i++) {
      a[i] = 0xff;
    }
    assert.equal(encode(a), 'vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv');
  });

  test('decode', () => {
    assert.equal(decode('00000000000000000000000000000000').length, 20);

    const a = alloc(20);
    assert.deepEqual(decode('00000000000000000000000000000000'), a);

    a[a.length - 1] = 1;
    assert.deepEqual(decode('00000000000000000000000000000001'), a);
    a[a.length - 1] = 10;
    assert.deepEqual(decode('0000000000000000000000000000000a'), a);
    a[a.length - 1] = 20;
    assert.deepEqual(decode('0000000000000000000000000000000k'), a);
    a[a.length - 1] = 31;
    assert.deepEqual(decode('0000000000000000000000000000000v'), a);
    a[a.length - 1] = 32;
    assert.deepEqual(decode('00000000000000000000000000000010'), a);
    a[a.length - 1] = 63;
    assert.deepEqual(decode('0000000000000000000000000000001v'), a);
    a[a.length - 1] = 64;
    assert.deepEqual(decode('00000000000000000000000000000020'), a);

    // Largest!
    for (let i = 0; i < a.length; i++) {
      a[i] = 0xff;
    }
    assert.deepEqual(decode('vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv'), a);
  });
});
