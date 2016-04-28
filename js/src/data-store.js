// @flow

import Chunk from './chunk.js';
import Ref from './ref.js';
import RefValue from './ref-value.js';
import {newStruct} from './struct.js';
import type {ChunkStore} from './chunk-store.js';
import type {NomsMap} from './map.js';
import type {NomsSet} from './set.js';
import type {valueOrPrimitive} from './value.js';
import {
  Field,
  makeRefType,
  makeStructType,
  makeSetType,
  makeMapType,
  Type,
  stringType,
  boolType,
  valueType,
  StructDesc,
} from './type.js';
import {newMap} from './map.js';
import {newSet} from './set.js';
import {decodeNomsValue} from './decode.js';
import {invariant} from './assert.js';
import {encodeNomsValue} from './encode.js';
import type {Commit} from './commit.js';

type DatasTypes = {
  commitType: Type,
  commitSetType: Type,
  refOfCommitType: Type,
  commitMapType: Type,
};

let emptyCommitMap: Promise<NomsMap<string, RefValue<Commit>>>;
function getEmptyCommitMap(): Promise<NomsMap<string, RefValue<Commit>>> {
  if (!emptyCommitMap) {
    emptyCommitMap = newMap([], getDatasTypes().commitMapType);
  }
  return emptyCommitMap;
}

let datasTypes: DatasTypes;
export function getDatasTypes(): DatasTypes {
  if (!datasTypes) {
    // struct Commit {
    //   value: Value
    //   parents: Set<Ref<Commit>>
    // }
    const commitType = makeStructType('Commit', [
      new Field('value', valueType, false),
    ]);
    const refOfCommitType = makeRefType(commitType);
    const commitSetType = makeSetType(refOfCommitType);
    invariant(commitType.desc instanceof StructDesc);
    commitType.desc.fields.push(new Field('parents', commitSetType, false));
    const commitMapType = makeMapType(stringType, refOfCommitType);
    datasTypes = {
      commitType,
      refOfCommitType,
      commitSetType,
      commitMapType,
    };
  }

  return datasTypes;
}

interface Cache<T> {  // eslint-disable-line no-undef
  entry(ref: Ref): ?CacheEntry<T>;  // eslint-disable-line no-undef
  get(ref: Ref): ?T;  // eslint-disable-line no-undef
  add(ref: Ref, size: number, value: T): void;  // eslint-disable-line no-undef
}

export default class DataStore {
  _cs: ChunkStore;
  _datasets: Promise<NomsMap<string, RefValue<Commit>>>;
  _valueCache: Cache<?valueOrPrimitive>;

  constructor(cs: ChunkStore, cacheSize: number = 0) {
    this._cs = cs;
    this._datasets = this._datasetsFromRootRef(cs.getRoot());
    this._valueCache = cacheSize > 0 ? new SizeCache(cacheSize) : new NoopCache();
  }

  _datasetsFromRootRef(rootRef: Promise<Ref>): Promise<NomsMap<string, RefValue<Commit>>> {
    return rootRef.then(rootRef => {
      if (rootRef.isEmpty()) {
        return getEmptyCommitMap();
      }

      return this.readValue(rootRef);
    });
  }

  head(datasetID: string): Promise<?Commit> {
    return this._datasets.then(
      datasets => datasets.get(datasetID).then(commitRef =>
          commitRef ? this.readValue(commitRef.targetRef) : null));
  }

  datasets(): Promise<NomsMap<string, RefValue<Commit>>> {
    return this._datasets;
  }

  async _descendsFrom(commit: Commit, currentHeadRef: RefValue<Commit>): Promise<boolean> {
    let ancestors = commit.parents;
    while (!(await ancestors.has(currentHeadRef))) {
      if (ancestors.isEmpty()) {
        return false;
      }
      ancestors = await getAncestors(ancestors, this);
    }
    return true;
  }

  // TODO: This should return Promise<?valueOrPrimitive>
  async readValue(ref: Ref): Promise<any> {
    const entry = this._valueCache.entry(ref);
    if (entry) {
      return entry.value;
    }
    const chunk: Chunk = await this._cs.get(ref);
    if (chunk.isEmpty()) {
      this._valueCache.add(ref, 0, null);
      return null;
    }

    const v = decodeNomsValue(chunk, this);
    this._valueCache.add(ref, chunk.data.length, v);
    return v;
  }

  writeValue<T: valueOrPrimitive>(v: T, t: ?Type = undefined): RefValue<T> {
    if (!t) {
      switch (typeof v) {
        case 'string':
          t = stringType;
          break;
        case 'boolean':
          t = boolType;
          break;
        case 'object':
          t = v.type;
          break;
        default:
          throw new Error(`type parameter is required for ${typeof v}`);
      }
      invariant(t);
    }
    const chunk = encodeNomsValue(v, t, this);
    invariant(!chunk.isEmpty());
    const {ref} = chunk;
    const refValue = new RefValue(ref, makeRefType(t));
    const entry = this._valueCache.entry(ref);
    if (entry && entry.present) {
      return refValue;
    }
    this._cs.put(chunk);
    this._valueCache.add(ref, chunk.data.length, v);
    return refValue;
  }

  async commit(datasetId: string, commit: Commit): Promise<DataStore> {
    const currentRootRefP = this._cs.getRoot();
    const datasetsP = this._datasetsFromRootRef(currentRootRefP);
    let currentDatasets = await (datasetsP:Promise<NomsMap>);
    const currentRootRef = await currentRootRefP;
    const commitRef = this.writeValue(commit);

    if (!currentRootRef.isEmpty()) {
      const currentHeadRef = await currentDatasets.get(datasetId);
      if (currentHeadRef) {
        if (commitRef.equals(currentHeadRef)) {
          return this;
        }
        if (!await this._descendsFrom(commit, currentHeadRef)) {
          throw new Error('Merge needed');
        }
      }
    }

    currentDatasets = await currentDatasets.set(datasetId, commitRef);
    const newRootRef = this.writeValue(currentDatasets).targetRef;
    if (await this._cs.updateRoot(newRootRef, currentRootRef)) {
      return new DataStore(this._cs);
    }

    throw new Error('Optimistic lock failed');
  }
}

async function getAncestors(commits: NomsSet<RefValue<Commit>>, store: DataStore):
    Promise<NomsSet<RefValue<Commit>>> {
  let ancestors = await newSet([], getDatasTypes().commitSetType);
  await commits.map(async (commitRef) => {
    const commit = await store.readValue(commitRef.targetRef);
    await commit.parents.map(async (ref) => ancestors = await ancestors.insert(ref));
  });
  return ancestors;
}

export function newCommit(value: valueOrPrimitive, parents: Array<Ref> = []): Promise<Commit> {
  const types = getDatasTypes();
  const parentRefs = parents.map(r => new RefValue(r, types.refOfCommitType));
  return newSet(parentRefs, types.commitSetType).then(parents =>
      newStruct(types.commitType, {value, parents}));
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
class SizeCache<T> {
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

class NoopCache<T> {
  entry(ref: Ref): ?CacheEntry {}  // eslint-disable-line no-unused-vars

  get(ref: Ref): ?T {}  // eslint-disable-line no-unused-vars

  add(ref: Ref, size: number, value: T) {}  // eslint-disable-line no-unused-vars
}
