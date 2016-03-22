// @flow

import {newCommit} from './data-store.js';
import type {valueOrPrimitive} from './value.js';
import type DataStore from './data-store.js';
import type Struct from './struct.js';
import type Ref from './ref.js';

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

  head(): Promise<?Struct> {
    return this._store.head(this._id);
  }

  // Commit updates the commit that a dataset points at. If parents is provided then an the promise
  // is rejected if the commit does not descend from the parents.
  async commit(v: valueOrPrimitive, parents: ?Array<Ref> = undefined): Promise<Dataset> {
    if (!parents) {
      const head = await this.head();
      parents = head ? [head.ref] : [];
    }
    const commit = await newCommit(v, parents);
    const store = await this._store.commit(this._id, commit);
    return new Dataset(store, this._id);
  }
}
