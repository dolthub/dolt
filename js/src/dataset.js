// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import Commit from './commit.js';
import type Value from './value.js';
import type Database from './database.js';
import Ref from './ref.js';
import Set from './set.js';

const idRe = /^[a-zA-Z0-9\-_/]+$/;

export default class Dataset {
  _database: Database;
  _id: string;

  constructor(database: Database, id: string) {
    if (!idRe.test(id)) {
      throw new TypeError(`Invalid dataset ID: ${id}`);
    }
    this._database = database;
    this._id = id;
  }

  get database(): Database {
    return this._database;
  }

  get id(): string {
    return this._id;
  }

  headRef(): Promise<?Ref<Commit>> {
    return this._database.headRef(this._id);
  }

  head(): Promise<?Commit> {
    return this._database.head(this._id);
  }

  headValue(): Promise<?Value> {
    return this.head().then(commit => commit && commit.value);
  }

  // Commit updates the commit that a dataset points at. If parents is provided then an the promise
  // is rejected if the commit does not descend from the parents.
  async commit(v: Value,
               parents: ?Array<Ref<Commit>> = undefined): Promise<Dataset> {
    if (!parents) {
      const headRef = await this.headRef();
      parents = headRef ? [headRef] : [];
    }
    const commit = new Commit(v, new Set(parents));
    const database = await this._database.commit(this._id, commit);
    return new Dataset(database, this._id);
  }
}
