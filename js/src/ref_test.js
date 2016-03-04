// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';
import Ref from './ref.js';
import {TextEncoder} from './text_encoding.js';


suite('Ref', () => {
  test('parse', () => {
    function assertParseError(s) {
      assert.throws(() => {
        Ref.parse(s);
      });
      assert.equal(null, Ref.maybeParse(s));
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
    assert.isNotNull(Ref.parse(valid));
    assert.isNotNull(Ref.maybeParse(valid));
  });

  test('equals', () => {
    const r0 = Ref.parse('sha1-0000000000000000000000000000000000000000');
    const r01 = Ref.parse('sha1-0000000000000000000000000000000000000000');
    const r1 = Ref.parse('sha1-0000000000000000000000000000000000000001');

    assert.isTrue(r0.equals(r01));
    assert.isTrue(r01.equals(r0));
    assert.isFalse(r0.equals(r1));
    assert.isFalse(r1.equals(r0));
  });

  test('toString', () => {
    const s = 'sha1-0123456789abcdef0123456789abcdef01234567';
    const r = Ref.parse(s);
    assert.strictEqual(s, r.toString());
  });

  test('fromData', () => {
    const r = Ref.fromData(new TextEncoder().encode('abc'));

    assert.strictEqual('sha1-a9993e364706816aba3e25717850c26c9cd0d89d', r.toString());
  });

  test('isEmpty', () => {
    const digest = new Uint8Array(20);
    let r = Ref.fromDigest(digest);
    assert.isTrue(r.isEmpty());

    digest[0] = 10;
    r = Ref.fromDigest(digest);
    assert.isFalse(r.isEmpty());

    r = new Ref();
    assert.isTrue(r.isEmpty());
  });
});
