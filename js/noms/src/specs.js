// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import AbsolutePath from './absolute-path.js';
import {notNull} from './assert.js';
import {BatchStoreAdaptor} from './batch-store.js';
import Dataset, {datasetRe} from './dataset.js';
import Database from './database.js';
import HttpBatchStore from './http-batch-store.js';
import MemoryStore from './memory-store.js';
import type Value from './value.js';

/**
 * A parsed specification for the location of a Noms database.
 * For example: 'mem' or 'https://demo.noms.io/cli-tour'.
 *
 * See https://github.com/attic-labs/noms/blob/master/doc/spelling.md.
 */
export class DatabaseSpec {
  scheme: string;
  path: string;

  _ms: MemoryStore | null;

  /**
   * Returns `spec` parsed as a DatabaseSpec. Throws a `SyntaxError` if `spec` isn't valid.
   */
  static parse(spec: string): DatabaseSpec {
    // A common mistake is to accidentally use the "ldb" protocol thinking it works,
    // because the Go SDK does.
    const ldbNotSupported = 'The "ldb" protocol is not supported in the JS SDK. ' +
      'Instead, use "noms serve" and point at "http://localhost:8000"';

    const protoIdx = spec.indexOf(':');
    if (protoIdx === -1) {
      if (spec === 'mem') {
        return new DatabaseSpec('mem', '');
      }
      // In the Go SDK this would be interpreted as "ldb", but JS doesn't support ldb.
      throw new SyntaxError(ldbNotSupported);
    }

    const protocol = spec.slice(0, protoIdx);
    const path = spec.slice(protoIdx + 1);

    switch (protocol) {
      case 'ldb':
        throw new SyntaxError(ldbNotSupported);
      case 'mem':
        throw new SyntaxError('In-memory database must be specified as "mem" not "mem:');
      case 'http':
      case 'https':
        // TODO: better validation, see https://github.com/attic-labs/noms/issues/2351.
        if (path === '') {
          throw new SyntaxError(`Invalid URL ${spec}`);
        }
        return new DatabaseSpec(protocol, path);
      default:
        throw new SyntaxError(`Unsupported protocol ${protocol}`);
    }
  }

  constructor(scheme: string, path: string) {
    this.scheme = scheme;
    this.path = path;
    // Cache the MemoryStore for testing, or it will reset every time database() is called.
    this._ms = scheme === 'mem' ? new MemoryStore() : null;
  }

  /**
   * Constructs a new Database based on the parsed spec.
   */
  database(): Database {
    switch (this.scheme) {
      case 'mem':
        return new Database(new BatchStoreAdaptor(notNull(this._ms)));
      case 'http':
      case 'https':
        return new Database(new HttpBatchStore(`${this.scheme}:${this.path}`));
      default:
        throw new Error('Unreached');
    }
  }
}

/**
 * A parsed specification for the location of a Noms dataset.
 * For example: 'mem::photos' or 'https://demo.noms.io/cli-tour::sf-crime'.
 *
 * See https://github.com/attic-labs/noms/blob/master/doc/spelling.md.
 */
export class DatasetSpec {
  database: DatabaseSpec;
  name: string;

  /**
   * Returns `spec` parsed as a DatasetSpec. Throws a `SyntaxError` if `spec` isn't valid.
   */
  static parse(spec: string): DatasetSpec {
    const [database, name] = splitAndParseDatabaseSpec(spec);
    if (!datasetRe.test(name)) {
      throw new SyntaxError(`Invalid dataset ${name}, must match ${datasetRe.source}`);
    }
    return new DatasetSpec(database, name);
  }

  constructor(database: DatabaseSpec, name: string) {
    this.database = database;
    this.name = name;
  }

  /**
   * Returns a new Dataset based on this DatasetSpec.
   */
  dataset(): Dataset {
    return new Dataset(this.database.database(), this.name);
  }

  /**
   * Returns a tuple of the database backed by this dataset, and value at the HEAD of this
   * dataset. If this dataset doesn't have a HEAD, the value will be `null`.
   *
   * The caller should always call `close()` when done.
   */
  value(): Promise<[Database, Value | null]> {
    const db = this.database.database();
    return this.dataset().head().then(commit => [db, commit ? commit.value : null]);
  }
}

/**
 * A path to a Noms value within a database.
 *
 * E.g. the entirety of a spec like `http://demo.noms.io::foo.bar` or
 * `http://demo.noms.io::#abcdef.bar`.
 */
export class PathSpec {
  database: DatabaseSpec;
  path: AbsolutePath;

  /**
   * Parses `str` as a PathSpec. Throws a SyntaxError if `str` isn't valid.
   */
  static parse(str: string): PathSpec {
    const [database, pathStr] = splitAndParseDatabaseSpec(str);
    const path = AbsolutePath.parse(pathStr);
    return new PathSpec(database, path);
  }

  constructor(database: DatabaseSpec, path: AbsolutePath) {
    this.database = database;
    this.path = path;
  }

  /**
   * Resolves this PathSpec, and returns the database it was resolved in, and the value it
   * resolved to. If the value wasn't found, it will be `null`.
   *
   * The caller should always call `close()` when done.
   */
  value(): Promise<[Database, Value|null]> {
    const db = this.database.database();
    return this.path.resolve(db).then(value => [db, value]);
  }
}

function splitAndParseDatabaseSpec(str: string): [DatabaseSpec, string] {
  const sep = '::';
  const sepIdx = str.lastIndexOf(sep);
  if (sepIdx === -1) {
    throw new SyntaxError(`Missing ${sep} separator between database and dataset: ${str}`);
  }
  const dbSpec = DatabaseSpec.parse(str.slice(0, sepIdx));
  return [dbSpec, str.slice(sepIdx + sep.length)];
}
