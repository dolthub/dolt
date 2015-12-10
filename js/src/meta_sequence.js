// @flow

import Ref from './ref.js';
import type {ChunkStore} from './chunk_store.js';
import type {NomsKind} from './noms_kind.js';
import {CompoundDesc, makeCompoundType, makePrimitiveType, Type} from './type.js';
import {invariant, notNull} from './assert.js';
import {Kind} from './noms_kind.js';
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

  readValue(cs: ChunkStore): Promise<any> {
    return readValue(this.ref, cs);
  }
}

export type metaBuilderFn = (cs: ChunkStore, t: Type, tuples: Array<MetaTuple>) => MetaSequence;

let metaFuncMap: Map<NomsKind, metaBuilderFn> = new Map();

export function newMetaSequenceFromData(cs: ChunkStore, t: Type, data: Array<MetaTuple>): MetaSequence {
  let ctor = notNull(metaFuncMap.get(t.kind));
  return ctor(cs, t, data);
}

export function registerMetaValue(k: NomsKind, bf: metaBuilderFn) {
  metaFuncMap.set(k, bf);
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
