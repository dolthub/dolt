// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';
import {newMapOfBoolToString} from './gen/map.noms.js';
import {makeMapType, boolType, stringType} from '@attic/noms';

suite('map.noms', () => {
  test('constructor', async () => {
    const s = await newMapOfBoolToString([true, 'yes', false, 'no']);
    assert.isTrue(s.type.equals(makeMapType(boolType, stringType)));
  });
});
