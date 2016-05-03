// @flow

import {newCommit} from './data-store.js';
import type {valueOrPrimitive} from './value.js';
import type {Commit} from './commit.js';
import type DataStore from './data-store.js';
import RefValue from './ref-value.js';

export default class Dataset {
  _store: DataStore;
  _id: string;

  constructor(store: DataStore, id: string) {
    this._store = store;
    this._id = id;
  }

  get store(): DataStore {
    return this._store;
  }

  get id(): string {
    return this._id;
  }

  headRef(): Promise<?RefValue<Commit>> {
    return this._store.headRef(this._id);
  }

  head(): Promise<?Commit> {
    return this._store.head(this._id);
  }

  // Commit updates the commit that a dataset points at. If parents is provided then an the promise
  // is rejected if the commit does not descend from the parents.
  async commit(v: valueOrPrimitive,
               parents: ?Array<RefValue<Commit>> = undefined): Promise<Dataset> {
    if (!parents) {
      const headRef = await this.headRef();
      parents = headRef ? [headRef] : [];
    }
    const commit: Commit = await newCommit(v, parents);
    const store = await this._store.commit(this._id, commit);
    return new Dataset(store, this._id);
  }
}
