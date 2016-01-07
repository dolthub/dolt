// @flow

import type {valueOrPrimitive} from './value.js'; // eslint-disable-line no-unused-vars
import {Collection} from './collection.js';
import {equals, less} from './value.js';
import {invariant} from './assert.js';
import {OrderedSequence} from './ordered_sequence.js';

export class NomsSet<T:valueOrPrimitive> extends Collection<OrderedSequence> {
  async has(key: T): Promise<boolean> {
    const cursor = await this.sequence.newCursorAt(this.cs, key);
    return cursor.valid && equals(cursor.getCurrentKey(), key);
  }

  async first(): Promise<?T> {
    const cursor = await this.sequence.newCursorAt(this.cs, null);
    return cursor.valid ? cursor.getCurrent() : null;
  }

  async forEach(cb: (v: T) => void): Promise<void> {
    const cursor = await this.sequence.newCursorAt(this.cs, null);
    return cursor.iter(v => {
      cb(v);
      return false;
    });
  }

  // TODO: Find some way to return a NomsSet.
  async map<S>(cb: (v: T) => (Promise<S> | S)): Promise<Array<S>> {
    const cursor = await this.sequence.newCursorAt(this.cs, null);
    const values = [];
    await cursor.iter(v => {
      values.push(cb(v));
      return false;
    });

    return Promise.all(values);
  }

  get size(): number {
    if (this.sequence instanceof SetLeafSequence) {
      return this.sequence.items.length;
    }

    throw new Error('not implemented');
  }

  async intersect(...sets: Array<NomsSet<T>>): Promise<NomsSet<T>> {
    if (sets.length === 0) {
      return this;
    }

    // Can't intersect sets of different element type.
    for (let i = 0; i < sets.length; i++) {
      invariant(sets[i].type.equals(this.type));
    }

    let cursor = await this.sequence.newCursorAt(this.cs, null);
    if (!cursor.valid) {
      return this;
    }

    const values: Array<T> = [];

    for (let i = 0; cursor.valid && i < sets.length; i++) {
      const first = cursor.getCurrent();
      const set: NomsSet = sets[i];
      const next = await set.sequence.newCursorAt(set.cs, first);
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
    return new NomsSet(this.cs, this.type, new SetLeafSequence(this.type, values));
  }
}

export class SetLeafSequence<K:valueOrPrimitive> extends OrderedSequence<K, K> {
  getKey(idx: number): K {
    return this.items[idx];
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
