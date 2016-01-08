// @flow

import {suite, test} from 'mocha';
import {assert} from 'chai';
import {encode, decode} from './base64.js';

function arrayBufferFromString(s: string): ArrayBuffer {
  const ta = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) {
    ta[i] = s.charCodeAt(i);
  }
  return ta.buffer;
}

suite('base64', () => {
  test('encode', () => {
    assert.deepEqual(encode(arrayBufferFromString('Hello world')), 'SGVsbG8gd29ybGQ=');
    assert.deepEqual(encode(arrayBufferFromString('Man')), 'TWFu');
    assert.deepEqual(encode(arrayBufferFromString('Ma')), 'TWE=');
    assert.deepEqual(encode(arrayBufferFromString('M')), 'TQ==');
    assert.deepEqual(encode(arrayBufferFromString('')), '');
    assert.deepEqual(encode(arrayBufferFromString('Hello worlds!')), 'SGVsbG8gd29ybGRzIQ==');
  });

  test('decode', () => {
    assert.deepEqual(decode('TWFu'), arrayBufferFromString('Man'));
    assert.deepEqual(decode('TWE='), arrayBufferFromString('Ma'));
    assert.deepEqual(decode('TQ=='), arrayBufferFromString('M'));
    assert.deepEqual(decode(''), arrayBufferFromString(''));
    assert.deepEqual(decode('SGVsbG8gd29ybGQ='), arrayBufferFromString('Hello world'));
    assert.deepEqual(decode('SGVsbG8gd29ybGRzIQ=='), arrayBufferFromString('Hello worlds!'));
  });
});
