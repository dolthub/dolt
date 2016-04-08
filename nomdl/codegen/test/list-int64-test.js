// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';
import {newListOfInt64} from './gen/list_int64.noms.js';
import {makeListType, int64Type} from '@attic/noms';

suite('list_int64.noms', () => {
  test('constructor', async () => {
    const l = await newListOfInt64([0, 1, 2, 3]);
    assert.equal(l.length, 4);
    assert.isTrue(l.type.equals(makeListType(int64Type)));
  });
});
