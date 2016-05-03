// @flow

import DataStore from './data-store.js';
import type {Collection} from './collection.js';
import type {valueOrPrimitive} from './value.js';
import {assert} from 'chai';
import {AsyncIterator} from './async-iterator.js';
import {getChunksOfValue, Value} from './value.js';
import {getRefOfValue} from './get-ref.js';
import {getTypeOfValue, Type} from './type.js';
import {makeTestingBatchStore} from './batch-store-adaptor.js';
import {notNull} from './assert.js';

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

export function assertValueRef(expectRefStr: string, v: valueOrPrimitive) {
  assert.strictEqual(expectRefStr, getRefOfValue(v).toString());
}

export function assertValueType(expectType: Type, v: valueOrPrimitive) {
  assert.isTrue(expectType.equals(getTypeOfValue(v)));
}

export function assertChunkCountAndType(expectCount: number, expectType: Type,
    v: Collection) {
  v.chunks.forEach(r => assert.isTrue(expectType.equals(r.type)));
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

export function chunkDiffCount(v1: valueOrPrimitive, v2: valueOrPrimitive): number {
  const c1 = getChunksOfValue(v1);
  const c2 = getChunksOfValue(v2);

  let diffCount = 0;
  const refs = Object.create(null);
  c1.forEach(r => {
    const refStr = r.targetRef.toString();
    let count = refs[refStr];
    count = count === undefined ? 1 : count + 1;
    refs[refStr] = count;
  });

  c2.forEach(r => {
    const refStr = r.targetRef.toString();
    const count = refs[refStr];
    if (count === undefined) {
      diffCount++;
    } else if (count === 1) {
      delete refs[refStr];
    } else {
      refs[refStr] = count - 1;
    }
  });

  return diffCount + Object.keys(refs).length;
}

export function intSequence(count: number, start: number = 0): Array<number> {
  const nums = [];

  for (let i = start; i < count; i++) {
    nums.push(i);
  }

  return nums;
}
