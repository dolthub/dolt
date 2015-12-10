  // @flow

import type {ChunkStore} from './chunk_store.js';
import type {valueOrPrimitive} from './value.js'; // eslint-disable-line no-unused-vars
import {IndexedSequence} from './indexed_sequence.js';
import {invariant} from './assert.js';
import {Kind} from './noms_kind.js';
import {MetaTuple, registerMetaValue} from './meta_sequence.js';
import {Type} from './type.js';

export class NomsList<K: valueOrPrimitive, T> extends IndexedSequence<T> {
  async get(idx: number): Promise<K> {
    invariant(idx < this.length, idx + ' >= ' + this.length);
    let cursor = await this.newCursorAt(idx);
    return cursor.getCurrent();
  }

  async forEach(cb: (v: K, i: number) => void): Promise<void> {
    let cursor = await this.newCursorAt(0);
    return cursor.iter((v, i) => {
      cb(v, i);
      return false;
    });
  }

  get length(): number {
    return this.items.length;
  }
}

export class ListLeaf<T: valueOrPrimitive> extends NomsList<T, T> {
  getOffset(idx: number): number {
    return idx;
  }
}

export class CompoundList<T: valueOrPrimitive> extends NomsList<T, MetaTuple<number>> {
  offsets: Array<number>;

  constructor(cs: ChunkStore, type: Type, items: Array<MetaTuple<number>>) {
    super(cs, type, items);
    this.isMeta = true;
    this.offsets = [];
    let cum = 0;
    for (let i = 0; i < items.length; i++) {
      let length = items[i].value;
      this.offsets.push(cum + length - 1);
      cum += length;
    }
  }

  getOffset(idx: number): number {
    return this.offsets[idx];
  }

  async getChildSequence(idx: number): Promise<?NomsList> {
    let mt = this.items[idx];
    let ms = await mt.readValue(this.cs);
    invariant(ms instanceof NomsList);
    return ms;
  }

  get length(): number {
    return this.offsets[this.items.length - 1] + 1;
  }
}

registerMetaValue(Kind.List, (cs, type, tuples) => new CompoundList(cs, type, tuples));

