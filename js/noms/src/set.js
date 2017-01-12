// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import type {ValueReader} from './value-store.js';
import type {makeChunkFn} from './sequence-chunker.js';
import type Value, {ValueCallback} from './value.js'; // eslint-disable-line no-unused-vars
import {AsyncIterator} from './async-iterator.js';
import {chunkSequence, chunkSequenceSync} from './sequence-chunker.js';
import Collection from './collection.js';
import {compare, equals} from './compare.js';
import {invariant} from './assert.js';
import Sequence, {OrderedKey} from './sequence.js';
import {newCursorAtIndex} from './indexed-sequence.js';
import {
  newOrderedMetaSequenceChunkFn,
} from './meta-sequence.js';
import {
  OrderedSequenceCursor,
  OrderedSequenceIterator,
  newCursorAt,
  newCursorAtValue,
} from './ordered-sequence.js';
import diff from './ordered-sequence-diff.js';
import {makeSetType, makeUnionType, getTypeOfValue} from './type.js';
import {removeDuplicateFromOrdered} from './map.js';
import {Kind} from './noms-kind.js';
import {hashValueBytes} from './rolling-value-hasher.js';

function newSetLeafChunkFn<T:Value>(vr: ?ValueReader): makeChunkFn<any, any> {
  return (items: Array<T>) => {
    const key = new OrderedKey(items.length > 0 ? items[items.length - 1] : false);
    const seq = newSetLeafSequence(vr, items);
    const ns = Set.fromSequence(seq);
    return [ns, key, items.length];
  };
}

function buildSetData<T: Value>(values: Array<any>): Array<T> {
  values = values.slice();
  values.sort(compare);
  return removeDuplicateFromOrdered(values, compare);
}

export function newSetLeafSequence<K: Value>(
    vr: ?ValueReader, items: K[]): SetLeafSequence<any> {
  const t = makeSetType(makeUnionType(items.map(getTypeOfValue)));
  return new SetLeafSequence(vr, t, items);
}

export default class Set<T: Value> extends Collection<Sequence<any>> {
  constructor(values: Array<T> = []) {
    const seq = chunkSequenceSync(
        buildSetData(values),
        newSetLeafChunkFn(null),
        newOrderedMetaSequenceChunkFn(Kind.Set, null),
        hashValueBytes);
    super(seq);
  }

  walkValues(vr: ValueReader, cb: ValueCallback): Promise<void> {
    return this.forEach(v => cb(v));
  }

  async has(key: T): Promise<boolean> {
    const cursor = await newCursorAtValue(this.sequence, key);
    return cursor.valid && equals(cursor.getCurrentKey().value(), key);
  }

  async _firstOrLast(last: boolean): Promise<?T> {
    const cursor = await newCursorAt(this.sequence, null, false, last);
    return cursor.valid ? cursor.getCurrent() : null;
  }

  first(): Promise<?T> {
    return this._firstOrLast(false);
  }

  last(): Promise<?T> {
    return this._firstOrLast(true);
  }

  // Returns the value at index `idx`, as per the (stable) ordering of noms
  // values in this set.
  at(idx: number): Promise<T> {
    invariant(idx >= 0 && idx < this.size);
    return newCursorAtIndex(this.sequence, idx).then(cur => cur.getCurrent());
  }

  async forEach(cb: (v: T) => ?Promise<any>): Promise<void> {
    const cursor = await newCursorAt(this.sequence, null, false, false, true);
    const promises = [];
    await cursor.iter(v => {
      promises.push(cb(v));
      return false;
    });
    await Promise.all(promises);
  }

  iterator(): AsyncIterator<T> {
    return new OrderedSequenceIterator(newCursorAt(this.sequence, null, false, false, true));
  }

  iteratorAt(v: T): AsyncIterator<T> {
    return new OrderedSequenceIterator(newCursorAtValue(this.sequence, v, false, false, true));
  }

  _splice(cursor: OrderedSequenceCursor<any, any>, insert: Array<T>, remove: number)
      : Promise<Set<T>> {
    const vr = this.sequence.vr;
    return chunkSequence(cursor, vr, insert, remove, newSetLeafChunkFn(vr),
                         newOrderedMetaSequenceChunkFn(Kind.Set, vr),
                         hashValueBytes).then(s => Set.fromSequence(s));
  }

  async add(value: T): Promise<Set<T>> {
    const cursor = await newCursorAtValue(this.sequence, value, true);
    if (cursor.valid && equals(cursor.getCurrentKey().value(), value)) {
      return this;
    }

    return this._splice(cursor, [value], 0);
  }

  async delete(value: T): Promise<Set<T>> {
    const cursor = await newCursorAtValue(this.sequence, value);
    if (cursor.valid && equals(cursor.getCurrentKey().value(), value)) {
      return this._splice(cursor, [], 1);
    }

    return this;
  }

  // TODO: Find some way to return a Set.
  async map<S>(cb: (v: T) => (Promise<S> | S)): Promise<Array<S>> {
    const cursor = await newCursorAt(this.sequence, null);
    const values = [];
    await cursor.iter(v => {
      values.push(cb(v));
      return false;
    });

    return Promise.all(values);
  }

  get size(): number {
    return this.sequence.numLeaves;
  }

  /**
   * Returns a 2-tuple [added, removed] sorted values.
   */
  diff(from: Set<T>): Promise<[Array<T> /* added */, Array<T> /* removed */]> {
    return diff(from.sequence, this.sequence).then(([added, removed, modified]) => {
      invariant(modified.length === 0);
      return [added, removed];
    });
  }
}

export class SetLeafSequence<K: Value> extends Sequence<K> {}
