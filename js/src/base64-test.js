// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {suite, test} from 'mocha';
import {assert} from 'chai';
import {encode, decode} from './base64.js';

function uint8ArrayFromString(s: string): Uint8Array {
  const ta = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) {
    ta[i] = s.charCodeAt(i);
  }
  return ta;
}

suite('base64', () => {
  test('encode', () => {
    assert.deepEqual(encode(uint8ArrayFromString('Hello world')), 'SGVsbG8gd29ybGQ=');
    assert.deepEqual(encode(uint8ArrayFromString('Man')), 'TWFu');
    assert.deepEqual(encode(uint8ArrayFromString('Ma')), 'TWE=');
    assert.deepEqual(encode(uint8ArrayFromString('M')), 'TQ==');
    assert.deepEqual(encode(uint8ArrayFromString('')), '');
    assert.deepEqual(encode(uint8ArrayFromString('Hello worlds!')), 'SGVsbG8gd29ybGRzIQ==');
  });

  test('decode', () => {
    assert.deepEqual(decode('TWFu'), uint8ArrayFromString('Man'));
    assert.deepEqual(decode('TWE='), uint8ArrayFromString('Ma'));
    assert.deepEqual(decode('TQ=='), uint8ArrayFromString('M'));
    assert.deepEqual(decode(''), uint8ArrayFromString(''));
    assert.deepEqual(decode('SGVsbG8gd29ybGQ='), uint8ArrayFromString('Hello world'));
    assert.deepEqual(decode('SGVsbG8gd29ybGRzIQ=='), uint8ArrayFromString('Hello worlds!'));
  });
});
