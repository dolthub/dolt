// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import type {ValueReader} from './value-store.js';
import {describeType} from './encode-human-readable.js';
import {getHashOfValue} from './get-hash.js';
import {Kind} from './noms-kind.js';
import type Hash from './hash.js';
import type {Type} from './type.js';
import type Value from './value.js'; // eslint-disable-line no-unused-vars
import {invariant} from './assert.js';
import {getTypeOfValue, makeRefType} from './type.js';
import {ValueBase, getChunksOfValue} from './value.js';

export function constructRef(t: Type<any>, targetHash: Hash, height: number): Ref<any> {
  invariant(t.kind === Kind.Ref, () => `Not a Ref type: ${describeType(t)}`);
  invariant(!targetHash.isEmpty());
  const rv = Object.create(Ref.prototype);
  rv._type = t;
  rv.targetHash = targetHash;
  rv.height = height;
  return rv;
}

export function maxChunkHeight(v: Value): number {
  return getChunksOfValue(v).reduce((max, c) => Math.max(max, c.height), 0);
}

export default class Ref<T: Value> extends ValueBase {
  _type: Type<any>;
  // Hash of the value this points to.
  targetHash: Hash;
  // The length of the longest path of Refs to find any leaf in the graph.
  // By definition this must be > 0.
  height: number;

  constructor(val: T) {
    super();
    this._type = makeRefType(getTypeOfValue(val));
    this.height = 1 + maxChunkHeight(val);
    this.targetHash = getHashOfValue(val);
  }

  get type(): Type<any> {
    return this._type;
  }

  targetValue(vr: ValueReader): Promise<T> {
    return vr.readValue(this.targetHash);
  }

  get chunks(): Array<Ref<any>> {
    return [this];
  }
}
