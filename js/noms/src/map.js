// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {invariant} from './assert.js';
import Ref from './ref.js';
import type {ValueReader} from './value-store.js';
import type {makeChunkFn} from './sequence-chunker.js';
import type Value from './value.js'; // eslint-disable-line no-unused-vars
import type {AsyncIterator} from './async-iterator.js';
import {chunkSequence, chunkSequenceSync} from './sequence-chunker.js';
import Collection from './collection.js';
import {compare, equals} from './compare.js';
import {getTypeOfValue, makeMapType, makeUnionType} from './type.js';
import {
  OrderedKey,
  newOrderedMetaSequenceChunkFn,
} from './meta-sequence.js';
import {
  OrderedSequence,
  OrderedSequenceCursor,
  OrderedSequenceIterator,
} from './ordered-sequence.js';
import diff from './ordered-sequence-diff.js';
import {ValueBase} from './value.js';
import {Kind} from './noms-kind.js';
import type {EqualsFn} from './edit-distance.js';
import RollingValueHasher, {hashValueBytes} from './rolling-value-hasher.js';

export type MapEntry<K: Value, V: Value> = [K, V];

const KEY = 0;
const VALUE = 1;

function newMapLeafChunkFn<K: Value, V: Value>(vr: ?ValueReader):
    makeChunkFn<any, any> {
  return (items: Array<MapEntry<K, V>>) => {
    const key = new OrderedKey(items.length > 0 ? items[items.length - 1][KEY] : false);
    const seq = newMapLeafSequence(vr, items);
    const nm = Map.fromSequence(seq);
    return [nm, key, seq.length];
  };
}

function mapHashValueBytes(entry: MapEntry<any, any>, rv: RollingValueHasher) {
  hashValueBytes(entry[KEY], rv);
  hashValueBytes(entry[VALUE], rv);
}

export function removeDuplicateFromOrdered<T>(elems: Array<T>,
    compare: (v1: T, v2: T) => number) : Array<T> {
  const unique = [];
  let i = -1;
  let last = null;
  elems.forEach((elem: T) => {
    if (null === elem || undefined === elem ||
        null === last || undefined === last || compare(last, elem) !== 0) {
      i++;
    }
    unique[i] = elem;
    last = elem;
  });

  return unique;
}

function compareKeys(v1, v2) {
  return compare(v1[KEY], v2[KEY]);
}

function buildMapData<K: Value, V: Value>(
    kvs: Array<MapEntry<K, V>>): Array<MapEntry<K, V>> {
  // TODO: Assert k & v are of correct type
  const entries = kvs.slice();
  entries.sort(compareKeys);
  return removeDuplicateFromOrdered(entries, compareKeys);
}

export default class Map<K: Value, V: Value> extends
    Collection<OrderedSequence<any, any>> {
  constructor(kvs: Array<MapEntry<K, V>> = []) {
    const seq = chunkSequenceSync(
        buildMapData(kvs),
        newMapLeafChunkFn(null),
        newOrderedMetaSequenceChunkFn(Kind.Map, null),
        mapHashValueBytes);
    invariant(seq instanceof OrderedSequence);
    super(seq);
  }

  async has(key: K): Promise<boolean> {
    const cursor = await this.sequence.newCursorAtValue(key);
    return cursor.valid && equals(cursor.getCurrentKey().value(), key);
  }

  async _firstOrLast(last: boolean): Promise<?MapEntry<K, V>> {
    const cursor = await this.sequence.newCursorAt(null, false, last);
    if (!cursor.valid) {
      return undefined;
    }

    return cursor.getCurrent();
  }

  first(): Promise<?MapEntry<K, V>> {
    return this._firstOrLast(false);
  }

  last(): Promise<?MapEntry<K, V>> {
    return this._firstOrLast(true);
  }

  async get(key: K): Promise<?V> {
    const cursor = await this.sequence.newCursorAtValue(key);
    if (!cursor.valid) {
      return undefined;
    }

    const entry = cursor.getCurrent();
    return equals(entry[KEY], key) ? entry[VALUE] : undefined;
  }

  async forEach(cb: (v: V, k: K) => ?Promise<any>): Promise<void> {
    const cursor = await this.sequence.newCursorAt(null);
    const promises = [];
    return cursor.iter(entry => {
      promises.push(cb(entry[VALUE], entry[KEY]));
      return false;
    }).then(() => Promise.all(promises)).then(() => void 0);
  }

  iterator(): AsyncIterator<MapEntry<K, V>> {
    return new OrderedSequenceIterator(this.sequence.newCursorAt(null));
  }

  iteratorAt(k: K): AsyncIterator<MapEntry<K, V>> {
    return new OrderedSequenceIterator(this.sequence.newCursorAtValue(k));
  }

  _splice(cursor: OrderedSequenceCursor<any, any>, insert: Array<MapEntry<K, V>>, remove: number):
      Promise<Map<K, V>> {
    const vr = this.sequence.vr;
    return chunkSequence(cursor, vr, insert, remove, newMapLeafChunkFn(vr),
                         newOrderedMetaSequenceChunkFn(Kind.Map, vr),
                         mapHashValueBytes).then(s => Map.fromSequence(s));
  }

  async set(key: K, value: V): Promise<Map<K, V>> {
    let remove = 0;
    const cursor = await this.sequence.newCursorAtValue(key, true);
    if (cursor.valid && equals(cursor.getCurrentKey().value(), key)) {
      const entry = cursor.getCurrent();
      if (equals(entry[VALUE], value)) {
        return this;
      }

      remove = 1;
    }

    return this._splice(cursor, [[key, value]], remove);
  }

  async delete(key: K): Promise<Map<K, V>> {
    const cursor = await this.sequence.newCursorAtValue(key);
    if (cursor.valid && equals(cursor.getCurrentKey().value(), key)) {
      return this._splice(cursor, [], 1);
    }

    return this;
  }

  get size(): number {
    return this.sequence.numLeaves;
  }

  /**
   * Returns a 3-tuple [added, removed, modified] sorted by keys.
   */
  diff(from: Map<K, V>): Promise<[Array<K>, Array<K>, Array<K>]> {
    return diff(from.sequence, this.sequence);
  }
}

export class MapLeafSequence<K: Value, V: Value> extends
    OrderedSequence<K, MapEntry<K, V>> {
  getKey(idx: number): OrderedKey<any> {
    return new OrderedKey(this.items[idx][KEY]);
  }

  getCompareFn(other: OrderedSequence<any, any>): EqualsFn {
    return (idx: number, otherIdx: number) =>
      equals(this.items[idx][KEY], other.items[otherIdx][KEY]) &&
      equals(this.items[idx][VALUE], other.items[otherIdx][VALUE]);
  }

  get chunks(): Array<Ref<any>> {
    const chunks = [];
    for (const entry of this.items) {
      if (entry[KEY] instanceof ValueBase) {
        chunks.push(...entry[KEY].chunks);
      }
      if (entry[VALUE] instanceof ValueBase) {
        chunks.push(...entry[VALUE].chunks);
      }
    }
    return chunks;
  }
}

export function newMapLeafSequence<K: Value, V: Value>(vr: ?ValueReader,
    items: Array<MapEntry<K, V>>): MapLeafSequence<K, V> {
  const kt = [];
  const vt = [];
  for (let i = 0; i < items.length; i++) {
    kt.push(getTypeOfValue(items[i][KEY]));
    vt.push(getTypeOfValue(items[i][VALUE]));
  }
  const t = makeMapType(makeUnionType(kt), makeUnionType(vt));
  return new MapLeafSequence(vr, t, items);
}
