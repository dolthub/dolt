// @flow

import type {ChunkStore} from './chunk_store.js';
import type {valueOrPrimitive} from './value.js'; // eslint-disable-line no-unused-vars
import {equals, less} from './value.js';
import {invariant} from './assert.js';
import {Kind} from './noms_kind.js';
import {OrderedSequence} from './ordered_sequence.js';
import {registerMetaValue, MetaTuple} from './meta_sequence.js';
import {Type} from './type.js';

export class NomsSet<K:valueOrPrimitive, T> extends OrderedSequence<K, T> {
  async first(): Promise<?T> {
    let cursor = await this.newCursorAt(null);
    return cursor.valid ? cursor.getCurrent() : null;
  }

  async forEach(cb: (v: T) => void): Promise<void> {
    let cursor = await this.newCursorAt(null);
    return cursor.iter(v => {
      cb(v);
      return false;
    });
  }

  get size(): number {
    return this.items.length;
  }

  async intersect(...sets: Array<NomsSet>): Promise<NomsSet> {
    if (sets.length === 0) {
      return this;
    }

    // Can't intersect sets of different element type.
    for (let i = 0; i < sets.length; i++) {
      invariant(sets[i].type.equals(this.type));
    }

    let cursor = await this.newCursorAt(null);
    if (!cursor.valid) {
      return this;
    }

    let values: Array<K> = [];

    for (let i = 0; cursor.valid && i < sets.length; i++) {
      let first = cursor.getCurrent();
      let next = await sets[i].newCursorAt(first);
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
    return new SetLeaf(this.cs, this.type, values);
  }
}

export class SetLeaf<K:valueOrPrimitive> extends NomsSet<K, K> {
  getKey(idx: number): K {
    return this.items[idx];
  }
}

export class CompoundSet<K:valueOrPrimitive> extends NomsSet<K, MetaTuple<K>> {
  constructor(cs: ChunkStore, type: Type, items: Array<MetaTuple>) {
    super(cs, type, items);
    this.isMeta = true;
  }

  getKey(idx: number): K {
    return this.items[idx].value;
  }

  async getChildSequence(idx: number): Promise<?SetLeaf> {
    let mt = this.items[idx];
    let ms = await mt.readValue(this.cs);
    invariant(ms instanceof NomsSet);
    return ms;
  }

  get size(): number {
    throw new Error('not implemented');
  }
}

registerMetaValue(Kind.Set, (cs, type, tuples) => new CompoundSet(cs, type, tuples));

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
