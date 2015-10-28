/* @flow */

'use strict';

import {assert} from 'chai';
import {suite, test} from 'mocha';
import Ref from './ref.js';

suite('Ref', () => {
  test('parse', () => {
    function assertParseError(s) {
      assert.throws(() => {
        Ref.parse(s);
      });
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

    let r = Ref.parse('sha1-0000000000000000000000000000000000000000');
    assert.isNotNull(r);
  });

  test('equals', () => {
    let r0 = Ref.parse('sha1-0000000000000000000000000000000000000000');
    let r01 = Ref.parse('sha1-0000000000000000000000000000000000000000');
    let r1 = Ref.parse('sha1-0000000000000000000000000000000000000001');

    assert.isTrue(r0.equals(r01));
    assert.isTrue(r01.equals(r0));
    assert.isFalse(r0.equals(r1));
    assert.isFalse(r1.equals(r0));
  });

  test('toString', () => {
    let s = 'sha1-0123456789abcdef0123456789abcdef01234567';
    let r = Ref.parse(s);
    assert.strictEqual(s, r.toString());
  });

  test('fromData', () => {
    let r = Ref.fromData('abc');

    assert.strictEqual('sha1-a9993e364706816aba3e25717850c26c9cd0d89d', r.toString());
  });

  test('isEmpty', () => {
    let digest = new Uint8Array(20);
    let r = new Ref(digest);
    assert.isTrue(r.isEmpty());

    digest[0] = 10;
    r = new Ref(digest);
    assert.isFalse(r.isEmpty());

    r = new Ref();
    assert.isTrue(r.isEmpty());
  });
});
