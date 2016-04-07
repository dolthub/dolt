// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';

import {Kind} from '@attic/noms';
import {Struct} from './gen/struct.noms.js';

suite('struct.noms', () => {
  test('constructor', () => {
    const s: Struct = new Struct({s: 'hi', b: true});
    assert.equal(s.s, 'hi');
    assert.equal(s.b, true);
  });

  test('type', () => {
    const s: Struct = new Struct({s: 'hi', b: true});
    assert.equal(s.type.kind, Kind.Unresolved);
  });
});
