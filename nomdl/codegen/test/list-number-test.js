// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';
import {newListOfNumber} from './gen/list_number.noms.js';
import {makeListType, numberType} from '@attic/noms';

suite('list_number.noms', () => {
  test('constructor', async () => {
    const l = await newListOfNumber([0, 1, 2, 3]);
    assert.equal(l.length, 4);
    assert.isTrue(l.type.equals(makeListType(numberType)));
  });
});
