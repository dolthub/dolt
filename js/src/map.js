// @flow

import BuzHashBoundaryChecker from './buzhash-boundary-checker.js';
import RefValue from './ref-value.js';
import type {ValueReader} from './value-store.js';
import type {BoundaryChecker, makeChunkFn} from './sequence-chunker.js';
import type {valueOrPrimitive} from './value.js'; // eslint-disable-line no-unused-vars
import type {AsyncIterator} from './async-iterator.js';
import {chunkSequence} from './sequence-chunker.js';
import {Collection} from './collection.js';
import {compare, equals} from './compare.js';
import {sha1Size} from './ref.js';
import {getRefOfValue} from './get-ref.js';
import {getTypeOfValue, makeMapType, makeUnionType} from './type.js';
import {MetaTuple, newOrderedMetaSequenceBoundaryChecker, newOrderedMetaSequenceChunkFn,} from
  './meta-sequence.js';
import {OrderedSequence, OrderedSequenceCursor, OrderedSequenceIterator,} from
  './ordered-sequence.js';
import diff from './ordered-sequence-diff.js';
import {Value} from './value.js';
import {Kind} from './noms-kind.js';

export type MapEntry<K: valueOrPrimitive, V: valueOrPrimitive> = [K, V];

const KEY = 0;
const VALUE = 1;

const mapWindowSize = 1;
const mapPattern = ((1 << 6) | 0) - 1;

function newMapLeafChunkFn<K: valueOrPrimitive, V: valueOrPrimitive>(vr: ?ValueReader):
    makeChunkFn {
  return (items: Array<MapEntry<K, V>>) => {
    let indexValue: ?valueOrPrimitive = null;
    if (items.length > 0) {
      indexValue = items[items.length - 1][KEY];
      if (indexValue instanceof Value) {
        indexValue = new RefValue(indexValue);
      }
    }

    const nm = new Map(newMapLeafSequence(vr, items));
    const mt = new MetaTuple(new RefValue(nm), indexValue, items.length, nm);
    return [mt, nm];
  };
}

function newMapLeafBoundaryChecker<K: valueOrPrimitive, V: valueOrPrimitive>():
    BoundaryChecker<MapEntry<K, V>> {
  return new BuzHashBoundaryChecker(mapWindowSize, sha1Size, mapPattern,
    (entry: MapEntry<K, V>) => getRefOfValue(entry[KEY]).digest);
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

function buildMapData<K: valueOrPrimitive, V: valueOrPrimitive>(
    kvs: Array<MapEntry<K, V>>): Array<MapEntry<K, V>> {
  // TODO: Assert k & v are of correct type
  const entries = [];
  for (let i = 0; i < kvs.length; i++) {
    entries.push([kvs[i][KEY], kvs[i][VALUE]]);
  }
  entries.sort(compareKeys);
  return removeDuplicateFromOrdered(entries, compareKeys);
}

export function newMap<K: valueOrPrimitive, V: valueOrPrimitive>(
    kvs: Array<MapEntry<K, V>>): Promise<Map<K, V>> {
  return chunkSequence(null, buildMapData(kvs), 0, newMapLeafChunkFn(null),
                       newOrderedMetaSequenceChunkFn(Kind.Map, null),
                       newMapLeafBoundaryChecker(),
                       newOrderedMetaSequenceBoundaryChecker);
}

export default class Map<K: valueOrPrimitive, V: valueOrPrimitive> extends
    Collection<OrderedSequence> {
  async has(key: K): Promise<boolean> {
    const cursor = await this.sequence.newCursorAt(key);
    return cursor.valid && equals(cursor.getCurrentKey(), key);
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
    const cursor = await this.sequence.newCursorAt(key);
    if (!cursor.valid) {
      return undefined;
    }

    const entry = cursor.getCurrent();
    return equals(entry[KEY], key) ? entry[VALUE] : undefined;
  }

  async forEach(cb: (v: V, k: K) => void): Promise<void> {
    const cursor = await this.sequence.newCursorAt(null);
    return cursor.iter(entry => {
      cb(entry[VALUE], entry[KEY]);
      return false;
    });
  }

  iterator(): AsyncIterator<MapEntry<K, V>> {
    return new OrderedSequenceIterator(this.sequence.newCursorAt(null));
  }

  iteratorAt(k: K): AsyncIterator<MapEntry<K, V>> {
    return new OrderedSequenceIterator(this.sequence.newCursorAt(k));
  }

  _splice(cursor: OrderedSequenceCursor, insert: Array<MapEntry<K, V>>, remove: number):
      Promise<Map<K, V>> {
    const vr = this.sequence.vr;
    return chunkSequence(cursor, insert, remove, newMapLeafChunkFn(vr),
                         newOrderedMetaSequenceChunkFn(Kind.Map, vr),
                         newMapLeafBoundaryChecker(),
                         newOrderedMetaSequenceBoundaryChecker);
  }

  async set(key: K, value: V): Promise<Map<K, V>> {
    let remove = 0;
    const cursor = await this.sequence.newCursorAt(key, true);
    if (cursor.valid && equals(cursor.getCurrentKey(), key)) {
      const entry = cursor.getCurrent();
      if (equals(entry[VALUE], value)) {
        return this;
      }

      remove = 1;
    }

    return this._splice(cursor, [[key, value]], remove);
  }

  async remove(key: K): Promise<Map<K, V>> {
    const cursor = await this.sequence.newCursorAt(key);
    if (cursor.valid && equals(cursor.getCurrentKey(), key)) {
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

export class MapLeafSequence<K: valueOrPrimitive, V: valueOrPrimitive> extends
    OrderedSequence<K, MapEntry<K, V>> {
  getKey(idx: number): K {
    return this.items[idx][KEY];
  }

  equalsAt(idx: number, other: MapEntry<K, V>): boolean {
    const entry = this.items[idx];
    return equals(entry[KEY], other[KEY]) && equals(entry[VALUE], other[VALUE]);
  }

  get chunks(): Array<RefValue> {
    const chunks = [];
    for (const entry of this.items) {
      if (entry[KEY] instanceof Value) {
        chunks.push(...entry[KEY].chunks);
      }
      if (entry[VALUE] instanceof Value) {
        chunks.push(...entry[VALUE].chunks);
      }
    }
    return chunks;
  }
}

export function newMapLeafSequence<K: valueOrPrimitive, V: valueOrPrimitive>(vr: ?ValueReader,
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
