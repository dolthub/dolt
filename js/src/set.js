// @flow

import type {ChunkStore} from './chunk_store.js';
import type {valueOrPrimitive} from './value.js'; // eslint-disable-line no-unused-vars
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
