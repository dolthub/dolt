// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {assert} from 'chai';
import {suite, test} from 'mocha';
import Hash, {emptyHash} from './hash.js';
import {encode} from './utf8.js';

suite('Hash', () => {
  test('parse', () => {
    function assertParseError(s) {
      assert.throws(() => {
        Hash.parse(s);
      });
      assert.equal(null, Hash.maybeParse(s));
    }

    assertParseError('foo');
    assertParseError('sha1');
    assertParseError('sha1-0');

    // too many digits
    assertParseError('sha1-00000000000000000000000000000000000000000');

    // 'g' not valid hex
    assertParseError('sha1- 000000000000000000000000000000000000000g');

    // sha2 not supported
    assertParseError('sha2-0000000000000000000000000000000000000000');

    const valid = 'sha1-0000000000000000000000000000000000000000';
    assert.isNotNull(Hash.parse(valid));
    assert.isNotNull(Hash.maybeParse(valid));
  });

  test('equals', () => {
    const r0 = Hash.parse('sha1-0000000000000000000000000000000000000000');
    const r01 = Hash.parse('sha1-0000000000000000000000000000000000000000');
    const r1 = Hash.parse('sha1-0000000000000000000000000000000000000001');

    assert.isTrue(r0.equals(r01));
    assert.isTrue(r01.equals(r0));
    assert.isFalse(r0.equals(r1));
    assert.isFalse(r1.equals(r0));
  });

  test('toString', () => {
    const s = 'sha1-0123456789abcdef0123456789abcdef01234567';
    const r = Hash.parse(s);
    assert.strictEqual(s, r.toString());
  });

  test('fromData', () => {
    const r = Hash.fromData(encode('abc'));

    assert.strictEqual('sha1-a9993e364706816aba3e25717850c26c9cd0d89d', r.toString());
  });

  test('isEmpty', () => {
    const digest = new Uint8Array(20);
    let r = Hash.fromDigest(digest);
    assert.isTrue(r.isEmpty());

    digest[0] = 10;
    r = Hash.fromDigest(digest);
    assert.isFalse(r.isEmpty());

    r = emptyHash;
    assert.isTrue(r.isEmpty());
  });
});
