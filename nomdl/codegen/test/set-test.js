// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';
import {newSetOfBool} from './gen/set.noms.js';
import {makeSetType, boolType} from '@attic/noms';

suite('set.noms', () => {
  test('constructor', async () => {
    const s = await newSetOfBool([true]);
    assert.isTrue(s.type.equals(makeSetType(boolType)));
  });
});
