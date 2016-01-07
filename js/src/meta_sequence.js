// @flow

import Ref from './ref.js';
import type {ChunkStore} from './chunk_store.js';
import type {valueOrPrimitive} from './value.js'; // eslint-disable-line no-unused-vars
import {CompoundDesc, makeCompoundType, makePrimitiveType, Type} from './type.js';
import {IndexedSequence} from './indexed_sequence.js';
import {invariant} from './assert.js';
import {Kind} from './noms_kind.js';
import {OrderedSequence} from './ordered_sequence.js';
import {readValue} from './read_value.js';
import {Sequence} from './sequence.js';

export type MetaSequence = Sequence<MetaTuple>;

export class MetaTuple<K> {
  ref: Ref;
  value: K;

  constructor(ref: Ref, value: K) {
    this.ref = ref;
    this.value = value;
  }
}

export class IndexedMetaSequence extends IndexedSequence<MetaTuple<number>> {
  offsets: Array<number>;

  constructor(type: Type, items: Array<MetaTuple<number>>) {
    super(type, items);
    this.isMeta = true;
    this.offsets = [];
    let cum = 0;
    for (let i = 0; i < items.length; i++) {
      let length = items[i].value;
      this.offsets.push(cum + length - 1);
      cum += length;
    }
  }

  async getChildSequence(cs: ChunkStore, idx: number): Promise<?IndexedSequence> {
    if (!this.isMeta) {
      return null;
    }

    let mt = this.items[idx];
    let collection = await readValue(mt.ref, cs);
    invariant(collection && collection.sequence instanceof IndexedSequence);
    return collection.sequence;
  }

  getOffset(idx: number): number {
    return this.offsets[idx];
  }
}

export class OrderedMetaSequence<K: valueOrPrimitive> extends OrderedSequence<K, MetaTuple<K>> {
  constructor(type: Type, items: Array<MetaTuple>) {
    super(type, items);
    this.isMeta = true;
  }

  async getChildSequence(cs: ChunkStore, idx: number): Promise<?OrderedSequence> {
    if (!this.isMeta) {
      return null;
    }

    let mt = this.items[idx];
    let collection = await readValue(mt.ref, cs);
    invariant(collection && collection.sequence instanceof OrderedSequence);
    return collection.sequence;
  }

  getKey(idx: number): K {
    return this.items[idx].value;
  }
}

export function newMetaSequenceFromData(cs: ChunkStore, type: Type, tuples: Array<MetaTuple>):
    MetaSequence {
  switch (type.kind) {
    case Kind.Map:
    case Kind.Set:
      return new OrderedMetaSequence(type, tuples);
    case Kind.List:
      return new IndexedMetaSequence(type, tuples);
    case Kind.Blob:
      throw new Error('Not implemented');
    default:
      throw new Error('Not reached');
  }
}

let indexedSequenceIndexType = makePrimitiveType(Kind.Uint64);

export function indexTypeForMetaSequence(t: Type): Type {
  switch (t.kind) {
    case Kind.Map:
    case Kind.Set: {
      let desc = t.desc;
      invariant(desc instanceof CompoundDesc);
      let elemType = desc.elemTypes[0];
      if (elemType.ordered) {
        return elemType;
      } else {
        return makeCompoundType(Kind.Ref, makePrimitiveType(Kind.Value));
      }
    }
    case Kind.Blob:
    case Kind.List:
      return indexedSequenceIndexType;
  }

  throw new Error('Not reached');
}
