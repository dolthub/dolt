// @flow

import Chunk from './chunk.js';
import Hash from './hash.js';
import {assert} from 'chai';
import {ensureHash, getHash} from './get-hash.js';
import {Kind} from './noms-kind.js';
import {suite, test} from 'mocha';

suite('get hash', () => {
  test('getHash', () => {
    const input = `t [${Kind.Bool},false]`;
    const hash = Chunk.fromString(input).hash;
    const actual = getHash(false);

    assert.strictEqual(hash.toString(), actual.toString());
  });

  test('ensureHash', () => {
    let h: ?Hash = null;
    h = ensureHash(h, false);
    assert.isNotNull(h);
    assert.strictEqual(h, ensureHash(h, false));
  });
});
