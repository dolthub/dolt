// @flow

import Chunk from './chunk.js';
import Ref, {emptyRef} from './ref.js';
import RefValue from './ref-value.js';
import type BatchStore from './batch-store.js';
import type Value from './value.js';
import {
  getTypeOfValue,
  Type,
  valueType,
} from './type.js';
import {Kind} from './noms-kind.js';
import {ValueBase} from './value.js';
import {decodeNomsValue} from './decode.js';
import {invariant, notNull} from './assert.js';
import {encodeNomsValue} from './encode.js';
import {describeType, describeTypeOfValue} from './encode-human-readable.js';
import {equals} from './compare.js';

export interface ValueWriter {
  writeValue<T: Value>(v: T, t: ?Type): RefValue<T>
}

export interface ValueReader {
  // TODO: This should return Promise<?Value>
  readValue(ref: Ref): Promise<any>
}

export interface ValueReadWriter {
  // TODO: This should return Promise<?Value>
  readValue(ref: Ref): Promise<any>;
  writeValue<T: Value>(v: T): RefValue<T>;
}

export default class ValueStore {
  _bs: BatchStore;
  _knownRefs: RefCache;
  _valueCache: Cache<?Value>;

  constructor(bs: BatchStore, cacheSize: number = 0) {
    this._bs = bs;
    this._knownRefs = new RefCache();
    this._valueCache = cacheSize > 0 ? new SizeCache(cacheSize) : new NoopCache();
  }

  // TODO: This should return Promise<?Value>
  async readValue(ref: Ref): Promise<any> {
    const entry = this._valueCache.entry(ref);
    if (entry) {
      return entry.value;
    }
    const chunk: Chunk = await this._bs.get(ref);
    if (chunk.isEmpty()) {
      this._valueCache.add(ref, 0, null);
      this._knownRefs.addIfNotPresent(ref, new RefCacheEntry(false));
      return null;
    }

    const v = decodeNomsValue(chunk, this);
    this._valueCache.add(ref, chunk.data.length, v);
    this._knownRefs.cacheChunks(v, ref);
    // ref is trivially a hint for v, so consider putting that in the cache.
    // If we got to v by reading some higher-level chunk, this entry gets dropped on
    // the floor because r already has a hint in the cache. If we later read some other
    // chunk that references v, cacheChunks will overwrite this with a hint pointing to that chunk.
    // If we don't do this, top-level Values that get read but not written -- such as the
    // existing Head of a Database upon a Commit -- can be erroneously left out during a pull.
    this._knownRefs.addIfNotPresent(ref, new RefCacheEntry(true, getTypeOfValue(v), ref));
    return v;
  }

  writeValue<T: Value>(v: T): RefValue<T> {
    const t = getTypeOfValue(v);
    const chunk = encodeNomsValue(v, this);
    invariant(!chunk.isEmpty());
    const {ref} = chunk;
    const refValue = new RefValue(v);
    const entry = this._knownRefs.get(ref);
    if (entry && entry.present) {
      return refValue;
    }
    const hints = this._knownRefs.checkChunksInCache(v);
    this._bs.schedulePut(chunk, hints);
    this._knownRefs.add(ref, new RefCacheEntry(true, t));
    return refValue;
  }

  async flush(): Promise<void> {
    return this._bs.flush();
  }

  close(): Promise<void> {
    return this._bs.close();
  }
}

interface Cache<T> {  // eslint-disable-line no-undef
  entry(ref: Ref): ?CacheEntry<T>;  // eslint-disable-line no-undef
  get(ref: Ref): ?T;  // eslint-disable-line no-undef
  add(ref: Ref, size: number, value: T): void;  // eslint-disable-line no-undef
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

  entry(ref: Ref): ?CacheEntry {
    const key = ref.toString();
    const entry = this._cache.get(key);
    if (!entry) {
      return undefined;
    }
    this._cache.delete(key);
    this._cache.set(key, entry);
    return entry;
  }

  get(ref: Ref): ?T {
    const entry = this.entry(ref);
    return entry ? entry.value : undefined;
  }

  add(ref: Ref, size: number, value: ?T) {
    const key = ref.toString();
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
}

export class NoopCache<T> {
  entry(ref: Ref): ?CacheEntry {}  // eslint-disable-line no-unused-vars

  get(ref: Ref): ?T {}  // eslint-disable-line no-unused-vars

  add(ref: Ref, size: number, value: T) {}  // eslint-disable-line no-unused-vars
}


class RefCacheEntry {
  present: boolean;
  type: ?Type;
  provenance: Ref;

  constructor(present: boolean = false, type: ?Type = null, provenance: Ref = emptyRef) {
    invariant((!present && !type) || (present && type), `present = ${present}, type = ${type}`);
    this.present = present;
    this.type = type;
    this.provenance = provenance;
  }
}

class RefCache {
  _cache: Map<string, RefCacheEntry>;

  constructor() {
    this._cache = new Map();
  }

  get(ref: Ref): ?RefCacheEntry {
    return this._cache.get(ref.toString());
  }

  add(ref: Ref, entry: RefCacheEntry) {
    this._cache.set(ref.toString(), entry);
  }

  addIfNotPresent(ref: Ref, entry: RefCacheEntry) {
    const refStr = ref.toString();
    const cur = this._cache.get(refStr);
    if (!cur || cur.provenance.isEmpty()) {
      this._cache.set(refStr, entry);
    }
  }

  cacheChunks(v: Value, ref: Ref) {
    if (v instanceof ValueBase) {
      v.chunks.forEach(reachable => {
        const hash = reachable.targetRef;
        const cur = this.get(hash);
        if (!cur || cur.provenance.isEmpty() || cur.provenance.equals(hash)) {
          this.add(hash, new RefCacheEntry(true, getTargetType(reachable), ref));
        }
      });
    }
  }

  checkChunksInCache(v: Value): Set<Ref> {
    const hints = new Set();
    if (v instanceof ValueBase) {
      const chunks = v.chunks;
      for (let i = 0; i < chunks.length; i++) {
        const reachable = chunks[i];
        const entry = this.get(reachable.targetRef);
        invariant(entry && entry.present, () =>
          `Value to write -- Type ${describeTypeOfValue(v)} -- contains ref ` +
          `${reachable.targetRef.toString()}, which points to a non-existent Value.`);
        if (!entry.provenance.isEmpty()) {
          hints.add(entry.provenance);
        }

        // BUG 1121
        // It's possible that entry.type will be simply 'Value', but that 'reachable' is actually a
        // properly-typed object -- that is, a Ref to some specific Type. The Exp below would fail,
        // though it's possible that the Type is actually correct. We wouldn't be able to verify
        // without reading it, though, so we'll dig into this later.
        const targetType = getTargetType(reachable);
        if (equals(targetType, valueType)) {
          continue;
        }
        const entryType = notNull(entry.type);
        invariant(equals(entryType, targetType), () =>
          `Value to write contains ref ${reachable.targetRef.toString()}, which points to a ` +
          `value of a different type: ${describeType(entryType)} != ${describeType(targetType)}`);
      }
    }
    return hints;
  }
}

function getTargetType(refVal: RefValue): Type {
  invariant(refVal.type.kind === Kind.Ref, refVal.type.kind);
  return refVal.type.elemTypes[0];
}
