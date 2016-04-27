// @flow

import {assert} from 'chai';
import {suite, test} from 'mocha';

import {newSet, makeSetType, numberType, DataStore, MemoryStore} from '@attic/noms';
import type {NomsSet, number} from '@attic/noms';
import {StructWithRef} from './gen/ref.noms.js';

suite('ref.noms', () => {
  test('constructor', async () => {
    const ds = new DataStore(new MemoryStore());
    const set: NomsSet<number> = await newSet([0, 1, 2, 3], makeSetType(numberType));
    const r = ds.writeValue(set);
    const struct = new StructWithRef({r});

    assert.isTrue(struct.r.equals(r));
    const set2 = await ds.readValue(r.targetRef);
    assert.isTrue(set.equals(set2));
  });
});
