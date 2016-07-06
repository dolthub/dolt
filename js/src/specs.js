// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {BatchStoreAdaptor} from './batch-store.js';
import Dataset from './dataset.js';
import Database from './database.js';
import HttpBatchStore from './http-batch-store.js';
import MemoryStore from './memory-store.js';
import Hash from './hash.js';

// A parsed specification for the location of a Noms database.
// For example: 'mem:' or 'https://ds.noms.io/aa/music'
//
// See "spelling databases" for details on supported syntaxes:
// https://docs.google.com/document/d/1QgKcRS304llwU0ECahKtn8lGBFmT5zXzWr-5tah1S_4/edit
export class DatabaseSpec {
  scheme: string;
  path: string;

  // Returns parsed spec, or null if the spec was invalid.
  static parse(spec: string): ?DatabaseSpec {
    const match = spec.match(/^(.+?)(:.+)?$/);
    if (!match) {
      return null;
    }
    const [, scheme, p] = match;
    if (p && p.indexOf('::') > 0) {
      return null;
    }
    const path = p ? p.substr(1) : p;
    switch (scheme) {
      case 'http':
      case 'https':
        if (!path) {
          return null;
        }
        break;
      case 'mem':
        if (path) {
          return null;
        }
        break;
      default:
        return null;
    }
    return new this(scheme, path || '');
  }

  constructor(scheme: string, path: string) {
    this.scheme = scheme;
    this.path = path;
  }

  // Constructs a new Database based on the parsed spec.
  database(): Database {
    if (this.scheme === 'mem') {
      return new Database(new BatchStoreAdaptor(new MemoryStore()));
    }
    if (this.scheme === 'http') {
      return new Database(new HttpBatchStore(`${this.scheme}:${this.path}`));
    }
    throw new Error('Unreached');
  }
}

// A parsed specification for the location of a Noms dataset.
// For example: 'mem:photos' or 'https://ds.noms.io/aa/music:funk'
//
// See "spelling datasets" for details on supported syntaxes:
// https://docs.google.com/document/d/1QgKcRS304llwU0ECahKtn8lGBFmT5zXzWr-5tah1S_4/edit
export class DatasetSpec {
  database: DatabaseSpec;
  name: string;

  // Returns a parsed spec, or null if the spec was invalid.
  static parse(spec: string): ?DatasetSpec {
    const match = spec.match(/^(.+)\:\:([a-zA-Z0-9\-_/]+)$/);
    if (!match) {
      return null;
    }
    const [, dbSpec, dsName] = match;
    const database = DatabaseSpec.parse(dbSpec);
    if (!database) {
      return null;
    }
    return new this(database, dsName);
  }

  constructor(database: DatabaseSpec, name: string) {
    this.database = database;
    this.name = name;
  }

  // Returns a new DataSet based on the parsed spec.
  dataset(): Dataset {
    return new Dataset(this.database.database(), this.name);
  }

  // Returns the value at the HEAD of this dataset, if any, or null otherwise.
  value(): Promise<any> {
    // Hm. Calling dataset() creates a Database that we then toss into the ether, which means we
    // can't call close() on it. Ideally, we'd fix that.
    return this.dataset().head()
      .then(commit => commit && commit.value);
  }
}


// A parsed specification for the location of a Noms hash.
// For example: 'mem:sha1-5ba4be791d336d3184be7ee7dc598037f410ef96' or
// 'https://ds.noms.io/aa/music:sha1-3ff6ee6add3490621a8886608cc8423dba3cf7ca'
//
// See "spelling objects" for details on supported syntaxes:
// https://docs.google.com/document/d/1QgKcRS304llwU0ECahKtn8lGBFmT5zXzWr-5tah1S_4/edit
export class HashSpec {
  database: DatabaseSpec;
  hash: Hash;

  // Returns a parsed spec, or null if the spec was invalid.
  static parse(spec: string): ?HashSpec {
    const match = spec.match(/^(.+)\:\:([\-sh0-9a-fA-F]+)$/);
    if (!match) {
      return null;
    }

    const [, dbSpec, hashPart] = match;
    const hash = Hash.parse(hashPart);
    if (!hash) {
      return null;
    }

    const database = DatabaseSpec.parse(dbSpec);
    if (!database) {
      return null;
    }

    return new this(database, hash);
  }

  constructor(database: DatabaseSpec, hash: Hash) {
    this.database = database;
    this.hash = hash;
  }

  // Returns the value for the spec'd reference, if any, or null otherwise.
  value(): Promise<any> {
    const database = this.database.database();
    return database.readValue(this.hash).then(v => database.close().then(() => v));
  }
}

// Parses and returns the provided hash or dataset spec.
export function parseObjectSpec(spec: string): ?(DatasetSpec | HashSpec) {
  return HashSpec.parse(spec) || DatasetSpec.parse(spec);
}
