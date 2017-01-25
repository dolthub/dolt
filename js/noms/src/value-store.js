// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import Chunk from './chunk.js';
import Hash, {emptyHash} from './hash.js';
import {constructRef, maxChunkHeight} from './ref.js';
import type Ref from './ref.js';
import type {BatchStore} from './batch-store.js';
import type Value from './value.js';
import {
  getTypeOfValue,
  makeRefType,
  Type,
  valueType,
} from './type.js';
import {Kind} from './noms-kind.js';
import {ValueBase} from './value.js';
import {decodeValue} from './codec.js';
import {invariant, notNull} from './assert.js';
import {encodeValue} from './codec.js';
import {describeType, describeTypeOfValue} from './encode-human-readable.js';
import {equals} from './compare.js';

export interface ValueWriter {
  writeValue<T: Value>(v: T): Ref<T>;
}

export interface ValueReader {
  // TODO: This should return Promise<?Value>
  readValue(hash: Hash): Promise<any>;
}

export interface ValueReadWriter {
  // TODO: This should return Promise<?Value>
  readValue(hash: Hash): Promise<any>;
  writeValue<T: Value>(v: T): Ref<T>;
}

export default class ValueStore {
  _bs: BatchStore;
  _knownHashes: HashCache;
  _valueCache: Cache<Promise<?Value>>;
  _pendingPuts: Map<string, PendingChunk>;

  constructor(bs: BatchStore, cacheSize: number = 0) {
    this._bs = bs;
    this._knownHashes = new HashCache();
    this._valueCache = cacheSize > 0 ? new SizeCache(cacheSize) : new NoopCache();
    this._pendingPuts = new Map();
  }

  // TODO: This should return Promise<?Value>
  readValue(hash: Hash): Promise<any> {
    const entry = this._valueCache.entry(hash);
    if (entry) {
      return entry.value;
    }

    let resolveSize;
    const sizeP: Promise<number> = new Promise((resolveFn => {
      resolveSize = resolveFn;
    }));

    const pc = this._pendingPuts.get(hash.toString());
    const chunkP = pc ? Promise.resolve(pc.c) : this._bs.get(hash);

    const valueP = chunkP.then(chunk => {
      resolveSize(chunk.data.byteLength);

      if (chunk.isEmpty()) {
        this._knownHashes.addIfNotPresent(hash, new HashCacheEntry(false));
        return null;
      }

      const v = decodeValue(chunk, this);
      this._knownHashes.cacheChunks(v, hash, false);
      // hash is trivially a hint for v, so consider putting that in the cache.
      // If we got to v by reading some higher-level chunk, this entry gets dropped on
      // the floor because r already has a hint in the cache. If we later read some other
      // chunk that references v, cacheChunks will overwrite this with a hint pointing to that
      // chunk. If we don't do this, top-level Values that get read but not written -- such as
      // the existing Head of a Database upon a Commit -- can be erroneously left out during a
      // pull.
      this._knownHashes.addIfNotPresent(hash, new HashCacheEntry(true, getTypeOfValue(v), hash));
      return v;
    });

    this._valueCache.add(hash, sizeP, valueP);
    return valueP;
  }

  writeValue<T: Value>(v: T): Ref<T> {
    const t = getTypeOfValue(v);
    const chunk = encodeValue(v, this);
    invariant(!chunk.isEmpty());
    const {hash} = chunk;
    const height = maxChunkHeight(v) + 1;
    const ref = constructRef(makeRefType(getTypeOfValue(v)), hash, height);
    const entry = this._knownHashes.get(hash);
    if (entry && entry.present) {
      return ref;
    }
    const hints = this._knownHashes.checkChunksInCache(v);

    if (v instanceof ValueBase) {
      const chunks = v.chunks;
      for (let i = 0; i < chunks.length; i++) {
        const reachableHash = chunks[i].targetHash.toString();
        const pc = this._pendingPuts.get(reachableHash);
        if (pc) {
          this._bs.schedulePut(pc.c, pc.hints);
          this._pendingPuts.delete(reachableHash);
        }
      }
    }
    this._pendingPuts.set(hash.toString(), new PendingChunk(chunk, hints));
    this._knownHashes.cacheChunks(v, hash, true);
    this._knownHashes.add(hash, new HashCacheEntry(true, t), false);
    this._valueCache.drop(hash);
    return ref;
  }

  async flush(): Promise<void> {
    this._knownHashes.mergePendingHints();
    for (const [, pc] of this._pendingPuts) {
      this._bs.schedulePut(pc.c, pc.hints);
    }
    this._pendingPuts = new Map();
    return this._bs.flush();
  }

  close(): Promise<void> {
    return this._bs.close();
  }
}

class PendingChunk {
  c: Chunk;
  hints: Set<Hash>;

  constructor(c: Chunk, hints: Set<Hash>) {
    this.c = c;
    this.hints = hints;
  }
}

interface Cache<T> {  // eslint-disable-line no-undef
  entry(hash: Hash): ?CacheEntry<T>;  // eslint-disable-line no-undef
  add(hash: Hash, size: Promise<number>, value: T): void;  // eslint-disable-line no-undef
  drop(hash: Hash): void;  // eslint-disable-line no-undef
}

class CacheEntry<T> {
  size: Promise<number>;
  value: T;

  constructor(size: Promise<number>, value: T) {
    this.size = size;
    this.value = value;
  }

  get present(): boolean {
    return this.value !== null;
  }
}

/**
 * This uses a Map as an LRU cache. It uses the behavior that iteration of keys in a Map is done in
 * insertion order and any time a value is checked it is taken out and reinserted which puts it last
 * in the iteration.
 */
class SizeCache<T> {
  _maxSize: number;
  _cache: Map<string, CacheEntry<T>>;

  constructor(size: number) {
    this._maxSize = size;
    this._cache = new Map();
  }

  entry(hash: Hash): ?CacheEntry<any> {
    const key = hash.toString();
    const entry = this._cache.get(key);
    if (!entry) {
      return undefined;
    }
    this._cache.delete(key);
    this._cache.set(key, entry);
    return entry;
  }

  add(hash: Hash, size: Promise<number>, value: T): void {
    const key = hash.toString();
    let entry = this._cache.get(key);
    if (entry) {
      this._cache.delete(key);
    }

    entry = new CacheEntry(size, value);
    this._cache.set(key, entry);
    entry.size.then(() => this.expire());
  }

  expire() {
    const keys = [];
    const sizePs = [];

    for (const [key, {size}] of this._cache) {
      keys.push(key);
      sizePs.push(size);
    }

    Promise.all(sizePs).then(sizes => {
      let size = sizes.reduce((cum, v) => cum + v);
      for (let i = 0; i < keys.length && size > this._maxSize; i++) {
        size -= sizes[i];
        this._cache.delete(keys[i]);
      }
    });
  }

  drop(hash: Hash): void {
    const key = hash.toString();
    const entry = this._cache.get(key);
    if (entry) {
      this._cache.delete(key);
      entry.size.then(() => this.expire());
    }
  }
}

class NoopCache<T> {
  entry(hash: Hash): ?CacheEntry<any> {}  // eslint-disable-line no-unused-vars

  add(hash: Hash, size: Promise<number>, value: T): void {}  // eslint-disable-line no-unused-vars

  drop(hash: Hash): void {}  // eslint-disable-line no-unused-vars
}


class HashCacheEntry {
  present: boolean;
  type: ?Type<any>;
  provenance: Hash;

  constructor(present: boolean = false, type: ?Type<any> = null, provenance: Hash = emptyHash) {
    invariant((!present && !type) || (present && type),
        `present = ${String(present)}, type = ${String(type)}`);
    this.present = present;
    this.type = type;
    this.provenance = provenance;
  }
}

class HashCache {
  _cache: Map<string, HashCacheEntry>;
  _pending: Map<string, HashCacheEntry>;

  constructor() {
    this._cache = new Map();
    this._pending = new Map();
  }

  get(hash: Hash): ?HashCacheEntry {
    return this._cache.get(hash.toString());
  }

  add(hash: Hash, entry: HashCacheEntry, toPending: boolean) {
    if (toPending) {
      this._pending.set(hash.toString(), entry);
    } else {
      this._cache.set(hash.toString(), entry);
    }
  }

  mergePendingHints() {
    for (const [hashStr, entry] of this._pending) {
      this._cache.set(hashStr, entry);
    }
    this._pending = new Map();
  }

  addIfNotPresent(hash: Hash, entry: HashCacheEntry) {
    const hashStr = hash.toString();
    const cur = this._cache.get(hashStr);
    if (!cur || cur.provenance.isEmpty()) {
      this._cache.set(hashStr, entry);
    }
  }

  cacheChunks(v: Value, hash: Hash, toPending: boolean) {
    if (v instanceof ValueBase) {
      v.chunks.forEach(reachable => {
        const h = reachable.targetHash;
        const cur = this.get(h);
        if (!cur || cur.provenance.isEmpty() || cur.provenance.equals(h)) {
          this.add(h, new HashCacheEntry(true, getTargetType(reachable), hash), toPending);
        }
      });
    }
  }

  checkChunksInCache(v: Value): Set<Hash> {
    const hints = new Set();
    if (v instanceof ValueBase) {
      const chunks = v.chunks;
      for (let i = 0; i < chunks.length; i++) {
        const reachable = chunks[i];
        const entry = this.get(reachable.targetHash);
        invariant(entry && entry.present, () =>
          `Value to write -- Type ${describeTypeOfValue(v)} -- contains ref ` +
          `${reachable.targetHash.toString()}, which points to a non-existent Value.`);
        if (!entry.provenance.isEmpty()) {
          hints.add(entry.provenance);
        }

        const targetType = getTargetType(reachable);
        if (equals(targetType, valueType)) {
          continue;
        }
        // BUG 3099 - TODO: if equals(entryType, valueType) but !equals(targetType, valueType),
        // readValue(targetHash), check its type against targetType, and panic if there's a
        // mismatch.
        const entryType = notNull(entry.type);
        if (equals(entryType, valueType)) {
          continue;
        }
        invariant(equals(entryType, targetType), () =>
          `Value to write contains ref ${reachable.targetHash.toString()}, which points to a ` +
          `value of a different type: ${describeType(entryType)} != ${describeType(targetType)}`);
      }
    }
    return hints;
  }
}

function getTargetType(refVal: Ref<any>): Type<any> {
  invariant(refVal.type.kind === Kind.Ref, refVal.type.kind);
  return refVal.type.elemTypes[0];
}
