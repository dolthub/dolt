// @flow

import type {ChunkStore} from './chunk-store.js';
import type Chunk from './chunk.js';
import type Ref from './ref.js';

/**
 * This caches successful get and has on the underlying store.
 */
export default class CacheStore {
  _store: ChunkStore;
  _chunkCache: Cache<Chunk>;
  _hasCache: Cache<boolean>;

  constructor(store: ChunkStore, size: number = 512) {
    this._store = store;
    this._chunkCache = new Cache(size);
    this._hasCache = new Cache(size);
  }

  getRoot(): Promise<Ref> {
    return this._store.getRoot();
  }

  updateRoot(current: Ref, last: Ref): Promise<boolean> {
    return this._store.updateRoot(current, last);
  }

  get(ref: Ref): Promise<Chunk> {
    const s = ref.toString();
    const chunk = this._chunkCache.get(s);
    if (chunk) {
      return Promise.resolve(chunk);
    }

    return this._store.get(ref).then(chunk => {
      if (!chunk.isEmpty()) {
        this._chunkCache.add(s, chunk);
      }
      return chunk;
    });
  }

  has(ref: Ref): Promise<boolean> {
    const s = ref.toString();
    const b = this._hasCache.get(s);
    if (b) {
      return Promise.resolve(true);
    }


    const chunk = this._chunkCache.get(s);
    if (chunk) {
      this._hasCache.add(s, true);
      return Promise.resolve(true);
    }


    return this._store.has(ref).then(b => {
      if (b) {
        this._hasCache.add(s, true);
      }
      return b;
    });
  }

  put(c: Chunk): void {
    // TODO: Consider add the chunk to the _chunkCache here.
    this._store.put(c);
  }
}

/**
 * This uses a Map as an LRU cache. It uses the behavior that iteration of keys in a Map is done in
 * insertion order and any time a value is checked it is taken out and reinserted which puts it last
 * in the iteration.
 */
class Cache<T> {
  _size: number;
  _maxSize: number;
  _cache: Map<string, T>;

  constructor(size: number) {
    this._maxSize = size;
    this._cache = new Map();
  }

  get size(): number {
    return this._cache.size;
  }

  get(key: string): ?T {
    const value = this._cache.get(key);
    if (value !== undefined) {
      this._cache.delete(key);
      this._cache.set(key, value);
    }
    return value;
  }

  add(key: string, value: T) {
    if (this._cache.has(key)) {
      this._cache.delete(key);
    }
    this._cache.set(key, value);

    if (this._cache.size > this._maxSize) {
      for (const key of this._cache.keys()) {
        if (this._cache.size <= this._maxSize) {
          break;
        }
        this._cache.delete(key);
      }
    }
  }
}
