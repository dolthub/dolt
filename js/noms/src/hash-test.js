// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import Hash, {emptyHash} from './hash.js';
import {assert} from 'chai';
import * as Bytes from './bytes.js';
import {notNull} from './assert.js';
import {suite, test} from 'mocha';

suite('Hash', () => {
  test('parse', () => {
    function assertParseError(s) {
      assert.equal(null, Hash.parse(s));
    }

    assertParseError('foo');

    // too few digits
    assertParseError('0000000000000000000000000000000');

    // too many digits
    assertParseError('000000000000000000000000000000000');

    // 'w' not valid base32
    assertParseError('00000000000000000000000000000000w');

    // no prefix
    assertParseError('sha1-00000000000000000000000000000000');
    assertParseError('sha2-00000000000000000000000000000000');

    const valid = '00000000000000000000000000000000';
    assert.isNotNull(Hash.parse(valid));
  });

  test('equals', () => {
    const r0 = notNull(Hash.parse('00000000000000000000000000000000'));
    const r01 = notNull(Hash.parse('00000000000000000000000000000000'));
    const r1 = notNull(Hash.parse('00000000000000000000000000000001'));

    assert.isTrue(r0.equals(r01));
    assert.isTrue(r01.equals(r0));
    assert.isFalse(r0.equals(r1));
    assert.isFalse(r1.equals(r0));
  });

  test('toString', () => {
    const s = '0123456789abcdefghijklmnopqrstuv';
    const r = notNull(Hash.parse(s));
    assert.strictEqual(s, r.toString());
  });

  test('fromData', () => {
    const r = Hash.fromData(Bytes.fromString('abc'));
    assert.strictEqual('rmnjb8cjc5tblj21ed4qs821649eduie', r.toString());
  });

  test('isEmpty', () => {
    const digest = Bytes.alloc(20);
    let r = new Hash(digest);
    assert.isTrue(r.isEmpty());

    digest[0] = 10;
    r = new Hash(digest);
    assert.isFalse(r.isEmpty());

    r = emptyHash;
    assert.isTrue(r.isEmpty());
  });
});
