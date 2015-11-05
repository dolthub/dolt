/* @flow */

'use strict';

import Chunk from './chunk.js';
import Ref from './ref.js';
import {assert} from 'chai';
import {ensureRef, getRef} from './get_ref.js';
import {Kind} from './noms_kind.js';
import {makePrimitiveTypeRef} from './type_ref.js';
import {suite, test} from 'mocha';

suite('get ref', () => {
  test('getRef', () => {
    let input = `t [${Kind.Bool},false]`;
    let ref = Chunk.fromString(input).ref;
    let tr = makePrimitiveTypeRef(Kind.Bool);
    let actual = getRef(false, tr);

    assert.strictEqual(ref.toString(), actual.toString());
  });

  test('ensureRef', () => {
    let r: ?Ref = null;
    let tr = makePrimitiveTypeRef(Kind.Bool);
    r = ensureRef(r, false, tr);
    assert.isNotNull(r);
    assert.strictEqual(r, ensureRef(r, false, tr));
  });
});
