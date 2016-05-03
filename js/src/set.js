// @flow

import BuzHashBoundaryChecker from './buzhash-boundary-checker.js';
import RefValue from './ref-value.js';
import type {ValueReader} from './value-store.js';
import type {BoundaryChecker, makeChunkFn} from './sequence-chunker.js';
import type {valueOrPrimitive, Value} from './value.js'; // eslint-disable-line no-unused-vars
import {AsyncIterator} from './async-iterator.js';
import {chunkSequence} from './sequence-chunker.js';
import {Collection} from './collection.js';
import {getCompareFunction, equals, less} from './compare.js';
import {getRefOfValue} from './get-ref.js';
import {invariant} from './assert.js';
import {MetaTuple, newOrderedMetaSequenceBoundaryChecker,
  newOrderedMetaSequenceChunkFn, newLeafRefValue} from './meta-sequence.js';
import {OrderedSequence, OrderedSequenceCursor,
  OrderedSequenceIterator} from './ordered-sequence.js';
import diff from './ordered-sequence-diff.js';
import {setOfValueType, Type} from './type.js';
import {sha1Size} from './ref.js';
import {removeDuplicateFromOrdered} from './map.js';
import {getValueChunks} from './sequence.js';

const setWindowSize = 1;
const setPattern = ((1 << 6) | 0) - 1;

function newSetLeafChunkFn<T:valueOrPrimitive>(t: Type, vr: ?ValueReader = null): makeChunkFn {
  return (items: Array<T>) => {
    const setLeaf = new SetLeafSequence(vr, t, items);

    let indexValue: ?(T | RefValue) = null;
    if (items.length > 0) {
      const lastValue = items[items.length - 1];
      if (t.elemTypes[0].ordered) {
        indexValue = lastValue;
      } else {
        indexValue = new RefValue(getRefOfValue(lastValue));
      }
    }

    const mt = new MetaTuple(newLeafRefValue(setLeaf), indexValue, items.length, setLeaf);
    return [mt, setLeaf];
  };
}

function newSetLeafBoundaryChecker<T:valueOrPrimitive>(): BoundaryChecker<T> {
  return new BuzHashBoundaryChecker(setWindowSize, sha1Size, setPattern, (v: T) => {
    const ref = getRefOfValue(v);
    return ref.digest;
  });
}

function buildSetData<T>(t: Type, values: Array<any>): Array<T> {
  // TODO: Assert values are of correct type
  const compare = getCompareFunction(t.elemTypes[0]);
  values.sort(compare);
  return removeDuplicateFromOrdered(values, (v1, v2) => 0 === compare(v1, v2));
}

export function newSet<T:valueOrPrimitive>(values: Array<T>, type: Type = setOfValueType):
    Promise<NomsSet<T>> {

  return chunkSequence(null, buildSetData(type, values), 0, newSetLeafChunkFn(type),
                       newOrderedMetaSequenceChunkFn(type),
                       newSetLeafBoundaryChecker(),
                       newOrderedMetaSequenceBoundaryChecker)
  .then((seq: OrderedSequence) => new NomsSet(type, seq));
}

export class NomsSet<T:valueOrPrimitive> extends Collection<OrderedSequence> {
  async has(key: T): Promise<boolean> {
    const cursor = await this.sequence.newCursorAt(key);
    return cursor.valid && equals(cursor.getCurrentKey(), key);
  }

  async _firstOrLast(last: boolean): Promise<?T> {
    const cursor = await this.sequence.newCursorAt(null, false, last);
    return cursor.valid ? cursor.getCurrent() : null;
  }

  first(): Promise<?T> {
    return this._firstOrLast(false);
  }

  last(): Promise<?T> {
    return this._firstOrLast(true);
  }

  async forEach(cb: (v: T) => void): Promise<void> {
    const cursor = await this.sequence.newCursorAt(null);
    return cursor.iter(v => {
      cb(v);
      return false;
    });
  }

  iterator(): AsyncIterator<T> {
    return new OrderedSequenceIterator(this.sequence.newCursorAt(null));
  }

  iteratorAt(v: T): AsyncIterator<T> {
    return new OrderedSequenceIterator(this.sequence.newCursorAt(v));
  }

  async _splice(cursor: OrderedSequenceCursor, insert: Array<T>, remove: number):
      Promise<NomsSet<T>> {
    const type = this.type;
    const vr = this.sequence.vr;
    const seq = await chunkSequence(cursor, insert, remove, newSetLeafChunkFn(type, vr),
                                    newOrderedMetaSequenceChunkFn(type, vr),
                                    newSetLeafBoundaryChecker(type),
                                    newOrderedMetaSequenceBoundaryChecker);
    invariant(seq instanceof OrderedSequence);
    return new NomsSet(type, seq);
  }

  async insert(value: T): Promise<NomsSet<T>> {
    const cursor = await this.sequence.newCursorAt(value, true);
    if (cursor.valid && equals(cursor.getCurrentKey(), value)) {
      return this;
    }

    return this._splice(cursor, [value], 0);
  }

  async remove(value: T): Promise<NomsSet<T>> {
    const cursor = await this.sequence.newCursorAt(value);
    if (cursor.valid && equals(cursor.getCurrentKey(), value)) {
      return this._splice(cursor, [], 1);
    }

    return this;
  }

  // TODO: Find some way to return a NomsSet.
  async map<S>(cb: (v: T) => (Promise<S> | S)): Promise<Array<S>> {
    const cursor = await this.sequence.newCursorAt(null);
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

  async intersect(...sets: Array<NomsSet<T>>): Promise<NomsSet<T>> {
    if (sets.length === 0) {
      return this;
    }

    // Can't intersect sets of different element type.
    for (let i = 0; i < sets.length; i++) {
      invariant(sets[i].type.equals(this.type));
    }

    let cursor = await this.sequence.newCursorAt(null);
    if (!cursor.valid) {
      return this;
    }

    const values: Array<T> = [];

    for (let i = 0; cursor.valid && i < sets.length; i++) {
      const first = cursor.getCurrent();
      const next = await sets[i].sequence.newCursorAt(first);
      if (!next.valid) {
        break;
      }

      cursor = new SetIntersectionCursor(cursor, next);
      await cursor.align();
    }

    while (cursor.valid) {
      values.push(cursor.getCurrent());
      await cursor.advance();
    }

    // TODO: Chunk the resulting set.
    return new NomsSet(this.type, new SetLeafSequence(null, this.type, values));
  }

  /**
   * Returns a 2-tuple [added, removed] sorted values.
   */
  diff(from: NomsSet<T>): Promise<[Array<T> /* added */, Array<T> /* removed */]> {
    return diff(from.sequence, this.sequence).then(([added, removed, modified]) => {
      invariant(modified.length === 0);
      return [added, removed];
    });
  }
}

export class SetLeafSequence<K:valueOrPrimitive> extends OrderedSequence<K, K> {
  getKey(idx: number): K {
    return this.items[idx];
  }

  equalsAt(idx: number, other: any): boolean {
    return equals(this.items[idx], other);
  }

  get chunks(): Array<RefValue> {
    return getValueChunks(this.items);
  }
}

type OrderedCursor<K: valueOrPrimitive> = {
  valid: boolean;
  getCurrent(): K;
  advanceTo(key: K): Promise<boolean>;
  advance(): Promise<boolean>;
}

class SetIntersectionCursor<K: valueOrPrimitive> {
  s1: OrderedCursor<K>;
  s2: OrderedCursor<K>;
  valid: boolean;

  constructor(s1: OrderedCursor<K>, s2: OrderedCursor<K>) {
    invariant(s1.valid && s2.valid);
    this.s1 = s1;
    this.s2 = s2;
    this.valid = true;
  }

  getCurrent(): K {
    invariant(this.valid);
    return this.s1.getCurrent();
  }

  async align(): Promise<boolean> {
    let v1 = this.s1.getCurrent();
    let v2 = this.s2.getCurrent();

    while (!equals(v1, v2)) {
      if (less(v1, v2)) {
        if (!await this.s1.advanceTo(v2)) {
          return this.valid = false;
        }

        v1 = this.s1.getCurrent();
        continue;
      }

      if (!await this.s2.advanceTo(v1)) {
        return this.valid = false;
      }

      v2 = this.s2.getCurrent();
    }

    return this.valid = true;
  }

  async advanceTo(key: K): Promise<boolean> {
    invariant(this.valid);
    return this.valid = await this.s1.advanceTo(key) && await this.align();
  }

  async advance(): Promise<boolean> {
    invariant(this.valid);
    return this.valid = await this.s1.advance() && await this.align();
  }
}
