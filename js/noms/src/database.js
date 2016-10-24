// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import Hash from './hash.js';
import Ref from './ref.js';
import Map from './map.js';
import Set from './set.js';
import type Value from './value.js';
import type {RootTracker} from './chunk-store.js';
import ValueStore from './value-store.js';
import type {BatchStore} from './batch-store.js';
import Dataset from './dataset.js';
import Commit from './commit.js';
import type Struct from './struct.js';

type CommitOptions = {
  parents?: ?Array<Ref<Commit<any>>>,
  meta?: Struct | void,
};

/**
 * Database provides versioned storage for noms values. While Values can be
 * directly read and written from a Database, it is generally more appropriate
 * to read data by inspecting the head of a Dataset and write new data by
 * updating the head of a Dataset via commit(). Particularly, new
 * data is not guaranteed to be persistent until after a commit operation completes.
 * The Database API is stateful, meaning that calls to getDataset() or
 * datasets() occurring after a call to commit() will represent the result of the commit().
 */
export default class Database {
  _vs: ValueStore;
  _rt: RootTracker;
  _datasets: Promise<Map<string, Ref<Commit<any>>>>;

  constructor(bs: BatchStore, cacheSize: number = 0) {
    this._vs = new ValueStore(bs, cacheSize);
    this._rt = bs;
    this._datasets = this._datasetsFromRootRef(bs.getRoot());
  }

  _datasetsFromRootRef(rootRef: Promise<Hash>): Promise<Map<string, Ref<Commit<any>>>> {
    return rootRef.then(rootRef => {
      if (rootRef.isEmpty()) {
        return Promise.resolve(new Map());
      }

      return this.readValue(rootRef);
    });
  }

  /**
   * datasets returns the root of the database.
   */
  datasets(): Promise<Map<string, Ref<Commit<any>>>> {
    return this._datasets;
  }

  /**
   * getDataset returns a Dataset struct containing the current mapping of
   * id in the above Datasets Map.
   */
  getDataset(id: string): Dataset {
    return new Dataset(this, id, this.datasets().then(sets => sets.get(id)));
  }

  // TODO: This should return Promise<Value | null>
  /**
   * readValue returns a Promise of the value with the hash `hash` in this Database, or
   * null if it's not present.
   */
  async readValue(hash: Hash): Promise<any> {
    return this._vs.readValue(hash);
  }

  /**
   * writeValue writes v to this Database and returns its Ref. v is not guaranteed to be
   * durable until a subsequent call to commit().
   */
  writeValue<T: Value>(v: T): Ref<T> {
    return this._vs.writeValue(v);
  }

  async _descendsFrom(commit: Commit<any>, currentHeadRef: Ref<Commit<any>>): Promise<boolean> {
    let ancestors = commit.parents;
    while (!(await ancestors.has(currentHeadRef))) {
      if (ancestors.isEmpty()) {
        return false;
      }
      ancestors = await getAncestors(ancestors, this);
    }
    return true;
  }

  /**
   * Commit updates the commit that `ds.id` in this database points at. All values that have been
   * written to this database are guaranteed to be persistent after `commit()` returns. The new
   * `Commit` struct is constructed using `v`, `opts.parents`, and `opts.meta`. If `opts.parents`
   * is present and not `undefined` then the current head is used. If `opts.meta is present and not
   * `undefined` then a fully initialized empty `Struct` is used to create the `Commit` struct.
   * The returned `Dataset` is always the newest snapshot, regardless of
   * success or failure, and `datasets()` is updated to match backing storage
   * upon return as well. If the update cannot be performed, e.g., because
   * of a conflict, `commit` returns a rejected `Promise`.
   */
  async commit(ds: Dataset, v: Value, opts: CommitOptions = {}): Promise<Dataset> {
    let {parents} = opts;
    if (!parents) {
      const headRef = await ds.headRef();
      parents = headRef ? [headRef] : [];
    }

    const commit = new Commit(v, new Set(parents), opts.meta);
    try {
      const commitRefPromise = this._doCommit(ds.id, commit);
      await commitRefPromise;
      return new Dataset(this, ds.id, commitRefPromise);
    } finally {
      this._datasets = this._datasetsFromRootRef(this._rt.getRoot());
    }
  }

  async _doCommit(datasetId: string, commit: Commit<any>): Promise<Ref<any>> {
    const commitRef = this.writeValue(commit);

    for (;;) {
      const currentRootRefP = this._rt.getRoot();
      const currentRootRef = await currentRootRefP;
      let currentDatasets = await this._datasetsFromRootRef(currentRootRefP);
      if (!currentRootRef.isEmpty()) {
        const currentHeadRef = await currentDatasets.get(datasetId);
        if (currentHeadRef) {
          if (!await this._descendsFrom(commit, currentHeadRef)) {
            throw new Error('Merge needed');
          }
        }
      }

      currentDatasets = await currentDatasets.set(datasetId, commitRef);
      const newRootRef = this.writeValue(currentDatasets).targetHash;
      if (await this._rt.updateRoot(newRootRef, currentRootRef)) {
        break;
      }
    }
    return commitRef;
  }

  close(): Promise<void> {
    return this._vs.close();
  }
}

async function getAncestors(commits: Set<Ref<Commit<any>>>, database: Database)
    : Promise<Set<Ref<Commit<any>>>> {
  let ancestors = new Set();
  await commits.map(async (commitRef) => {
    const commit = await database.readValue(commitRef.targetHash);
    await commit.parents.map(async (ref) => ancestors = await ancestors.add(ref));
  });
  return ancestors;
}
