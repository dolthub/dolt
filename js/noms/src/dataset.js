// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import Commit from './commit.js';
import type Value from './value.js';
import type Database from './database.js';
import Ref from './ref.js';

/** Matches any valid dataset name in a string. */
export const datasetRe = /^[a-zA-Z0-9\-_/]+/;

/** Matches if an entire string is a valid dataset name. */
const idRe = new RegExp('^' + datasetRe.source + '$');

/** Dataset is a named Commit within a Database. */
export default class Dataset {
  _database: Database;
  _id: string;
  _headRef: Promise<?Ref<Commit<any>>>;

  constructor(database: Database, id: string, headRef: Promise<?Ref<Commit<any>>>) {
    if (!idRe.test(id)) {
      throw new TypeError(`Invalid dataset ID: ${id}`);
    }
    this._database = database;
    this._id = id;
    this._headRef = headRef;
  }

  // WARNING: database() is under consideration for deprecation.
  get database(): Database {
    return this._database;
  }

  /** id() returns the name of this Dataset. */
  get id(): string {
    return this._id;
  }

  // TODO: This should return Promise<Ref<Commit> | null>.
  /**
   * headRef returns the Ref of the current head Commit, which contains the
   * current root of the Dataset's value tree.
   * If the named Dataset doesn't exist in the underlying Database, the returned
   * Promise may resolve to null.
   * The returned Promise may be rejected if errors occur while getting the Ref.
   */
  headRef(): Promise<?Ref<Commit<any>>> {
    return this._headRef;
  }

  // TODO: This should return Promise<Commit | null>
  /**
   * head returns the current head Commit, which contains the current root of
   * the Dataset's value tree.
   * If the named Dataset doesn't exist in the underlying Database, the returned
   * Promise may resolve to null.
   * The returned Promise may be rejected if errors occur while getting the Head.
   */
  head(): Promise<?Commit<any>> {
    return this._headRef.then(hr => hr ? hr.targetValue(this._database) : null);
  }

  /**
   * headValue returns the Value field of the current head Commit.
   * If the named Dataset doesn't exist in underlying Database, the returned
   * Promise may resolve to null.
   * The returned Promise may be rejected if errors occur while getting the Head.
   */
  headValue(): Promise<?Value> {
    return this.head().then(commit => commit && commit.value);
  }
}
