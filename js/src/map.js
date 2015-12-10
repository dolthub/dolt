// @flow

import type {ChunkStore} from './chunk_store.js';
import type {valueOrPrimitive} from './value.js'; // eslint-disable-line no-unused-vars
import {equals} from './value.js';
import {invariant} from './assert.js';
import {Kind} from './noms_kind.js';
import {MetaTuple, registerMetaValue} from './meta_sequence.js';
import {OrderedSequence} from './ordered_sequence.js';
import {Type} from './type.js';

type Entry<K: valueOrPrimitive, V: valueOrPrimitive> = {
  key: K,
  value: V
};

export class NomsMap<K: valueOrPrimitive, V: valueOrPrimitive, T> extends OrderedSequence<K, T> {
  async first(): Promise<?[K, V]> {
    let cursor = await this.newCursorAt(null);
    if (!cursor.valid) {
      return undefined;
    }

    let entry = cursor.getCurrent();
    return [entry.key, entry.value];
  }

  async get(key: K): Promise<?V> {
    let cursor = await this.newCursorAt(key);
    if (!cursor.valid) {
      return undefined;
    }

    let entry = cursor.getCurrent();
    return equals(entry.key, key) ? entry.value : undefined;
  }

  async forEach(cb: (v: V, k: K) => void): Promise<void> {
    let cursor = await this.newCursorAt(null);
    return cursor.iter(entry => {
      cb(entry.value, entry.key);
      return false;
    });
  }

  get size(): number {
    return this.items.length;
  }
}

export class MapLeaf<K: valueOrPrimitive, V: valueOrPrimitive> extends NomsMap<K, V, Entry<K, V>> {
  getKey(idx: number): K {
    return this.items[idx].key;
  }
}

export class CompoundMap<K: valueOrPrimitive, V: valueOrPrimitive> extends NomsMap<K, V, MetaTuple<K>> {
  constructor(cs: ChunkStore, type: Type, items: Array<MetaTuple>) {
    super(cs, type, items);
    this.isMeta = true;
  }

  getKey(idx: number): K {
    return this.items[idx].value;
  }

  async getChildSequence(idx: number): Promise<?MapLeaf> {
    let mt = this.items[idx];
    let ms = await mt.readValue(this.cs);
    invariant(ms instanceof NomsMap);
    return ms;
  }

  get size(): number {
    throw new Error('not implemented');
  }
}

registerMetaValue(Kind.Map, (cs, type, tuples) => new CompoundMap(cs, type, tuples));

