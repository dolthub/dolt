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
  _valueCache: Cache<?Value>;

  constructor(bs: BatchStore, cacheSize: number = 0) {
    this._bs = bs;
    this._knownHashes = new HashCache();
    this._valueCache = cacheSize > 0 ? new SizeCache(cacheSize) : new NoopCache();
  }

  // TODO: This should return Promise<?Value>
  async readValue(hash: Hash): Promise<any> {
    const entry = this._valueCache.entry(hash);
    if (entry) {
      return entry.value;
    }
    const chunk: Chunk = await this._bs.get(hash);
    if (chunk.isEmpty()) {
      this._valueCache.add(hash, 0, null);
      this._knownHashes.addIfNotPresent(hash, new HashCacheEntry(false));
      return null;
    }

    const v = decodeValue(chunk, this);
    this._valueCache.add(hash, chunk.data.length, v);
    this._knownHashes.cacheChunks(v, hash);
    // hash is trivially a hint for v, so consider putting that in the cache.
    // If we got to v by reading some higher-level chunk, this entry gets dropped on
    // the floor because r already has a hint in the cache. If we later read some other
    // chunk that references v, cacheChunks will overwrite this with a hint pointing to that chunk.
    // If we don't do this, top-level Values that get read but not written -- such as the
    // existing Head of a Database upon a Commit -- can be erroneously left out during a pull.
    this._knownHashes.addIfNotPresent(hash, new HashCacheEntry(true, getTypeOfValue(v), hash));
    return v;
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
    this._bs.schedulePut(chunk, hints);
    this._knownHashes.add(hash, new HashCacheEntry(true, t));
    this._valueCache.drop(hash);
    return ref;
  }

  async flush(): Promise<void> {
    return this._bs.flush();
  }

  close(): Promise<void> {
    return this._bs.close();
  }
}

interface Cache<T> {  // eslint-disable-line no-undef
  entry(hash: Hash): ?CacheEntry<T>;  // eslint-disable-line no-undef
  get(hash: Hash): ?T;  // eslint-disable-line no-undef
  add(hash: Hash, size: number, value: T): void;  // eslint-disable-line no-undef
  drop(hash: Hash): void;  // eslint-disable-line no-undef
}

class CacheEntry<T> {
  size: number;
  value: ?T;

  constructor(size: number, value: ?T) {
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
export class SizeCache<T> {
  _size: number;
  _maxSize: number;
  _cache: Map<string, CacheEntry<T>>;

  constructor(size: number) {
    this._maxSize = size;
    this._cache = new Map();
    this._size = 0;
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

  get(hash: Hash): ?T {
    const entry = this.entry(hash);
    return entry ? entry.value : undefined;
  }

  add(hash: Hash, size: number, value: ?T): void {
    const key = hash.toString();
    if (this._cache.has(key)) {
      this._cache.delete(key);
    } else {
      this._size += size;
    }
    this._cache.set(key, new CacheEntry(size, value));

    if (this._size > this._maxSize) {
      for (const [key, {size}] of this._cache) {
        if (this._size <= this._maxSize) {
          break;
        }
        this._cache.delete(key);
        this._size -= size;
      }
    }
  }

  drop(hash: Hash): void {
    const key = hash.toString();
    const entry = this._cache.get(key);
    if (entry) {
      this._cache.delete(key);
      this._size -= entry.size;
    }
  }
}

export class NoopCache<T> {
  entry(hash: Hash): ?CacheEntry<any> {}  // eslint-disable-line no-unused-vars

  get(hash: Hash): ?T {}  // eslint-disable-line no-unused-vars

  add(hash: Hash, size: number, value: T): void {}  // eslint-disable-line no-unused-vars

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

  constructor() {
    this._cache = new Map();
  }

  get(hash: Hash): ?HashCacheEntry {
    return this._cache.get(hash.toString());
  }

  add(hash: Hash, entry: HashCacheEntry) {
    this._cache.set(hash.toString(), entry);
  }

  addIfNotPresent(hash: Hash, entry: HashCacheEntry) {
    const hashStr = hash.toString();
    const cur = this._cache.get(hashStr);
    if (!cur || cur.provenance.isEmpty()) {
      this._cache.set(hashStr, entry);
    }
  }

  cacheChunks(v: Value, hash: Hash) {
    if (v instanceof ValueBase) {
      v.chunks.forEach(reachable => {
        const h = reachable.targetHash;
        const cur = this.get(h);
        if (!cur || cur.provenance.isEmpty() || cur.provenance.equals(h)) {
          this.add(h, new HashCacheEntry(true, getTargetType(reachable), hash));
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

        // BUG 1121
        // It's possible that entry.type will be simply 'Value', but that 'reachable' is actually a
        // properly-typed object -- that is, a Hash to some specific Type. The Exp below would fail,
        // though it's possible that the Type is actually correct. We wouldn't be able to verify
        // without reading it, though, so we'll dig into this later.
        const targetType = getTargetType(reachable);
        if (equals(targetType, valueType)) {
          continue;
        }
        const entryType = notNull(entry.type);
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
