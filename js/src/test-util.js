// @flow

import DataStore from './data-store.js';
import type {valueOrPrimitive} from './value.js';
import {AsyncIterator} from './async-iterator.js';
import {Value} from './value.js';
import {assert} from 'chai';
import {notNull} from './assert.js';
import {makeTestingBatchStore} from './batch-store-adaptor.js';

export async function flatten<T>(iter: AsyncIterator<T>): Promise<Array<T>> {
  const values = [];
  for (let next = await iter.next(); !next.done; next = await iter.next()) {
    values.push(notNull(next.value));
  }
  return values;
}

export async function flattenParallel<T>(iter: AsyncIterator<T>, count: number): Promise<Array<T>> {
  const promises = [];
  for (let i = 0; i < count; i++) {
    promises.push(iter.next());
  }
  const results = await Promise.all(promises);
  return results.map(res => notNull(res.value));
}

export async function testRoundTripAndValidate<T: valueOrPrimitive>(v: T,
      validateFn: (v2: T) => Promise<void>): Promise<void> {
  const bs = makeTestingBatchStore();
  const ds = new DataStore(bs);

  const r1 = await ds.writeValue(v).targetRef;
  const ds2 = new DataStore(bs);

  const v2 = await ds2.readValue(r1);
  if (v instanceof Value) {
    assert.isTrue(v.equals(v2));
    assert.isTrue(v2.equals(v));
  } else {
    assert.strictEqual(v2, v);
  }
  await validateFn(v2);
}