// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';
import {EnumStruct} from './gen/enum_struct.noms.js';
import type {Handedness} from './gen/enum_struct.noms.js';

suite('enum_struct.noms', () => {
  test('constructor', async () => {
    const es = new EnumStruct({hand: 0});
    assert.equal(es.hand, 0);
    const hand: Handedness = es.hand;
    assert.equal(es.hand, hand);
    const es2 = es.setHand(1);
    assert.equal(es2.hand, 1);
  });
});
