// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {assert} from 'chai';
import {suite, test} from 'mocha';
import {encodingLength, encode, decode, maxVarintLength} from './signed-varint.js';
import {alloc} from './bytes.js';

suite('signed varint', () => {
  test('encodingLength', () => {
    assert.equal(encodingLength(0), 1);

    const buf = alloc(maxVarintLength);
    for (let i = 0; i < 54; i++) {
      const n2 = Math.pow(2, i);
      for (const n of [n2, -n2, n2 + 1, n2 - 1, -n2 - 1, -n2 + 1]) {
        assert.equal(encodingLength(n), encode(n, buf, 0));
      }
    }
  });

  test('encoding', () => {
    const buf = alloc(maxVarintLength);
    assert.equal(encode(0, buf, 0), 1);
    assert.equal(buf[0], 0);

    // Make sure we write 0
    buf[0] = 255;
    assert.equal(encode(0, buf, 0), 1);
    assert.equal(buf[0], 0);

    assert.equal(encode(1, buf, 0), 1);
    assert.equal(buf[0], 2);

    assert.equal(encode(0x3f, buf, 0), 1);
    assert.equal(buf[0], 126);

    // offset
    assert.equal(encode(1, buf, 1), 1);
    assert.equal(buf[1], 2);

    assert.equal(encode(-1, buf, 0), 1);
    assert.equal(buf[0], 1);

    assert.equal(encode(127, buf, 0), 2);
    assert.equal(buf[0], 254);
    assert.equal(buf[1], 1);
  });

  test('decoding', () => {
    const buf = alloc(maxVarintLength);
    assert.deepEqual(decode(buf, 0), [0, 1]);

    buf[0] = 2;
    assert.deepEqual(decode(buf, 0), [1, 1]);

    // offset
    buf[1] = 2;
    assert.deepEqual(decode(buf, 1), [1, 1]);
    assert.equal(buf[1], 2);

    buf[0] = 1;
    assert.deepEqual(decode(buf, 0), [-1, 1]);

    buf[0] = 254;
    buf[1] = 1;
    assert.deepEqual(decode(buf, 0), [127, 2]);
  });

  test('roundtrip', () => {
    const vs = [
      0,
      1,
      0xffffffff,
      42,
    ];

    for (let i = 0; i < 54; i++) {
      vs.push(Math.pow(2, i) - 1);
      vs.push(Math.pow(2, i));
      vs.push(Math.pow(2, i) + 1);
    }

    const buf = alloc(maxVarintLength);
    for (const v2 of vs) {
      for (const v of [v2, -v2]) {
        encode(v, buf, 0);
        assert.equal(decode(buf, 0)[0], v);
      }
    }
  });
});
