// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import Database from './database.js';
import type Collection from './collection.js';
import type Value from './value.js';
import {assert} from 'chai';
import {notNull} from './assert.js';
import {AsyncIterator} from './async-iterator.js';
import {getChunksOfValue, ValueBase} from './value.js';
import {getHashOfValue} from './get-hash.js';
import {getTypeOfValue, Type} from './type.js';
import {equals} from './compare.js';
import {BatchStoreAdaptor} from './batch-store.js';
import MemoryStore from './memory-store.js';
import type Ref from './ref.js';

export class TestDatabase extends Database {
  writeCount: number;

  constructor() {
    super(new BatchStoreAdaptor(new MemoryStore()));
    this.writeCount = 0;
  }

  writeValue<T: Value>(v: T): Ref<T> {
    this.writeCount++;
    return super.writeValue(v);
  }
}

export async function flatten<T>(iter: AsyncIterator<T>): Promise<Array<T>> {
  const values = [];
  for (let next = await iter.next(); !next.done; next = await iter.next()) {
    values.push(notNull(next.value));
  }
  return values;
}

export async function flattenParallel<T>(iter: AsyncIterator<T>, count: number):
    Promise<Array<T>> {
  const promises = [];
  for (let i = 0; i < count; i++) {
    promises.push(iter.next());
  }
  const results = await Promise.all(promises);
  return results.map(res => notNull(res.value));
}

export function assertValueHash(expectHashStr: string, v: Value) {
  assert.strictEqual(expectHashStr, getHashOfValue(v).toString());
}

export function assertValueType(expectType: Type<any>, v: Value) {
  assert.isTrue(equals(expectType, getTypeOfValue(v)));
}

export function assertChunkCountAndType(expectCount: number, expectType: Type<any>,
    v: Collection<any>) {
  const chunks = v.chunks;
  assert.strictEqual(expectCount, chunks.length);
  v.chunks.forEach(r => assert.isTrue(equals(expectType, r.type)));
}

export async function testRoundTripAndValidate<T: Value>(v: T,
      validateFn: (v2: T) => Promise<void>): Promise<void> {
  const ms = new MemoryStore();
  const ds = new Database(new BatchStoreAdaptor(ms));

  const r1 = await ds.writeValue(v).targetHash;
  const ds2 = new Database(new BatchStoreAdaptor(ms));

  const v2 = await ds2.readValue(r1);
  if (v instanceof ValueBase) {
    assert.isTrue(equals(v, v2));
    assert.isTrue(equals(v2, v));
  } else {
    assert.strictEqual(v2, v);
  }
  await validateFn(v2);
  await ds2.close();
}

export function chunkDiffCount(v1: Value, v2: Value): number {
  const c1 = getChunksOfValue(v1);
  const c2 = getChunksOfValue(v2);

  let diffCount = 0;
  const hashes = Object.create(null);
  c1.forEach(r => {
    const hashStr = r.targetHash.toString();
    let count = hashes[hashStr];
    count = count === undefined ? 1 : count + 1;
    hashes[hashStr] = count;
  });

  c2.forEach(r => {
    const hashStr = r.targetHash.toString();
    const count = hashes[hashStr];
    if (count === undefined) {
      diffCount++;
    } else if (count === 1) {
      delete hashes[hashStr];
    } else {
      hashes[hashStr] = count - 1;
    }
  });

  return diffCount + Object.keys(hashes).length;
}

export function intSequence(count: number, start: number = 0): Array<number> {
  const nums = [];

  for (let i = start; i < count; i++) {
    nums.push(i);
  }

  return nums;
}

export function deriveCollectionHeight(col: Collection<any>): number {
  // Note: not using seq.items[0].ref.height because the purpose of this method is to
  // be redundant.
  return col.sequence.isMeta ? 1 + deriveCollectionHeight(notNull(col.sequence.items[0].child)) : 0;
}
