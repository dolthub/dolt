// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import AbsolutePath from './absolute-path.js';
import {invariant} from './assert.js';
import {BatchStoreAdaptor} from './batch-store.js';
import Dataset, {datasetRe} from './dataset.js';
import Database from './database.js';
import HttpBatchStore, {DEFAULT_MAX_READS} from './http-batch-store.js';
import MemoryStore from './memory-store.js';
import type Value from './value.js';

// TODO: Change databaseName() -> get databaseName, database() -> getDatabase().
// TODO: Change databaseName to include prefix // for http/https protocols,
//       e.g. http://example.com -> databaseName = //example.com
// TODO: Add utility for getting the hostname or whatever out of database names which are URLs.

export type SpecOptions = {
  /**
   * Authorization token for requests. For example, if the Database is backed by
   * HTTP this will used for an `Authorization: Bearer ${authorization}` header.
   */
  authorization?: string;

  /**
   * If this is specified and > 0, the Database instances returned by
   * `database()` will be backed by a cache.
   */
  cacheSize?: number;
};

/**
 * Spec is a parsed specification for the location of a Noms database, dataset,
 * or value.
 *
 * Spec has the same `Database` instance for its construction, and the
 * accessors have the same mutability semantics as the `Database` interface,
 * which is mutable. For example, if the underlying `Database` commits a
 * dataset, then calls to `Spec.dataset()` will return the new one.
 *
 * For example:
 *  - a database spec might be `mem` or `https://demo.noms.io/cli-tour`.
 *  - a dataset spec might be `mem::photos` or `https://demo.noms.io/cli-tour::sf-crime`.
 *  - a path spec might be `http://demo.noms.io::foo.bar` or `http://demo.noms.io::#abcdef.bar`
 *
 * See https://github.com/attic-labs/noms/blob/master/doc/spelling.md.
 */
export default class Spec {
  _spec: string;
  _opts: ?SpecOptions;
  _protocol: string;
  _databaseName: string;

  // Lazily created.
  _database: ?Database;

  // Exists if the spec was constructed through `forDataset()`.
  _datasetName: ?string;

  // Exists if the spec was constructed through `fromValue()` or `pin()`.
  _path: ?AbsolutePath;

  constructor(spec: string, databaseSpec: string, opts: ?SpecOptions) {
    this._spec = spec;
    this._opts = opts;
    const [protocol, databaseName] = parseDatabaseSpec(databaseSpec);
    this._protocol = protocol;
    this._databaseName = databaseName;
  }

  /**
   * Creates a `Spec` from a spec to a database.
   */
  static forDatabase(spec: string, opts: ?SpecOptions): Spec {
    return new Spec(spec, spec, opts);
  }

  /**
   * Creates a `Spec` from a spec to a dataset.
   */
  static forDataset(spec: string, opts: ?SpecOptions): Spec {
    const [databaseSpec, datasetName] = splitDatabaseSpec(spec);
    if (!datasetRe.test(datasetName)) {
      throw new SyntaxError(`Invalid dataset ${datasetName}, must match ${datasetRe.source}`);
    }

    const specObj = new Spec(spec, databaseSpec, opts);
    specObj._datasetName = datasetName;
    return specObj;
  }

  /**
   * Creates a `Spec` from a spec to a value path.
   */
  static forPath(spec: string, opts: ?SpecOptions): Spec {
    const [databaseSpec, pathStr] = splitDatabaseSpec(spec);
    const path = AbsolutePath.parse(pathStr);

    const specObj = new Spec(spec, databaseSpec, opts);
    specObj._path = path;
    return specObj;
  }

  /**
   * Returns the spec string.
   */
  spec(): string {
    return this._spec;
  }

  /**
   * Returns the database protocol, for example `"mem"` or `"http"`.
   */
  protocol(): string {
    return this._protocol;
  }

  /**
   * Returns the database name, e.g. `"demo.noms.io"` or `"localhost:8000"`.
   */
  databaseName(): string {
    return this._databaseName;
  }

  /**
   * Returns the `Database`.
   */
  database(): Database {
    if (!this._database) {
      this._database = this._createDatabase();
    }
    return this._database;
  }

  /**
   * Returns the dataset name, for example `"sf-crime"`.
   *
   * Throws a `TypeError` if this spec does not have an associated value.
   */
  datasetName(): string {
    const name = this._datasetName;
    if (name === undefined || name === null) {
      throw new TypeError(`${this._spec} is not a dataset spec`);
    }
    return name;
  }

  /**
   * Returns the path with this spec's `Database`. Throws a `TypeError` if this
   * spec does not have an associated path.
   */
  path(): AbsolutePath {
    if (!this._path) {
      throw new TypeError(`${this._spec} is not a path spec`);
    }
    return this._path;
  }

  /**
   * Returns the `Dataset` from the spec.
   *
   * Throws a `TypeError` if this spec does not have a associated dataset.
   *
   * Make sure to call `close()`.
   */
  dataset(): Dataset {
    return this.database().getDataset(this.datasetName());
  }

  /**
   * Resolves this to a `Value`, or `null` if the value isn't found.
   *
   * Throws a `TypeError` if this spec does not have an associated value.
   *
   * Make sure to call `close()`.
   */
  value(): Promise<Value | null> {
    return this.path().resolve(this.database());
  }

  /**
   * Returns a `Spec` in which the dataset component, if any, has been replaced
   * with the hash of the HEAD of that dataset. This "pins" the path to the
   * state of the database at the current moment in time.
   *
   * Returns this if the PathSpec is already "pinned".
   *
   * Throws a `TypeError` if this spec does not have an associated dataset.
   */
  async pin(): Promise<?Spec> {
    let dataset;

    const path = this._path;
    if (path) {
      if (path.hash !== null) {
        // Spec is already pinned.
        invariant(this._datasetName === undefined || this._datasetName === null);
        return this;
      }

      dataset = this.database().getDataset(path.dataset);
    } else {
      dataset = this.dataset();
    }

    const commit = await dataset.head();
    if (!commit) {
      return null;
    }

    let spec = `${this._protocol}`;
    if (this._databaseName !== '') {
      spec += `://${this._databaseName}`;
    }

    spec += `::#${commit.hash.toString()}`;

    if (this._path && this._path.path !== null) {
      spec += this._path.path.toString();
    }

    const pinned = Spec.forPath(spec, this._opts);
    pinned._database = this._database;

    return pinned;
  }

  /**
   * Closes the database backed by this spec, if any.
   */
  close(): Promise<void> {
    const db = this._database;
    if (!db) {
      return Promise.resolve();
    }

    delete this._database;
    return db.close();
  }

  _createDatabase(): Database {
    let batchStore;

    switch (this._protocol) {
      case 'mem':
        batchStore = new BatchStoreAdaptor(new MemoryStore());
        break;

      case 'http':
      case 'https': {
        let fetchOptions;
        if (this._opts && this._opts.authorization) {
          fetchOptions = {
            headers: {
              'Authorization': `Bearer ${this._opts.authorization}`,
            },
          };
        }
        const url = `${this._protocol}://${this._databaseName}`;
        batchStore = new HttpBatchStore(url, DEFAULT_MAX_READS, fetchOptions);
        break;
      }

      default:
        throw new Error('unreachable');
    }

    let cacheSize;
    if (this._opts && this._opts.cacheSize) {
      cacheSize = this._opts.cacheSize;
    }

    return new Database(batchStore, cacheSize);
  }
}

function parseDatabaseSpec(spec: string): [string, string] {
  // A common mistake is to accidentally use the "ldb" protocol thinking it works,
  // because the Go SDK does.
  const ldbNotSupported = 'The "ldb" protocol is not supported in the JS SDK. ' +
    'Instead, use "noms serve" and point at "http://localhost:8000"';

  const protoIdx = spec.indexOf(':');

  if (protoIdx === -1) {
    if (spec !== 'mem') {
      // In the Go SDK this would be interpreted as "ldb", but JS doesn't support ldb.
      throw new SyntaxError(ldbNotSupported);
    }
    return ['mem', ''];
  }

  const protocol = spec.slice(0, protoIdx);
  const databaseName = spec.slice(protoIdx + 1);

  switch (protocol) {
    case 'ldb':
      throw new SyntaxError(ldbNotSupported);

    case 'http':
    case 'https':
      // TODO: better validation, see https://github.com/attic-labs/noms/issues/2351.
      if (databaseName === '') {
        throw new SyntaxError(`Invalid URL ${spec}`);
      }
      return [protocol, databaseName.replace(/^\/\//, '')];

    case 'mem':
      throw new SyntaxError('In-memory database must be specified as "mem" not "mem:');

    default:
      throw new SyntaxError(`Unsupported protocol ${protocol}`);
  }
}

function splitDatabaseSpec(str: string): [string /* database */, string /* rest */] {
  const sep = '::';
  const sepIdx = str.lastIndexOf(sep);
  if (sepIdx === -1) {
    throw new SyntaxError(`Missing ${sep} separator after database in ${str}`);
  }

  return [str.slice(0, sepIdx), str.slice(sepIdx + sep.length)];
}
