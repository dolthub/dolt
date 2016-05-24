// @flow

import BatchStoreAdaptor from './batch-store-adaptor.js';
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
    const match = spec.match(/^(.+?)(\:.+)?$/);
    if (!match) {
      return null;
    }
    const [, scheme, path] = match;
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
    return new this(scheme, (path || '').substr(1));
  }

  constructor(scheme: string, path: string) {
    this.scheme = scheme;
    this.path = path;
  }

  // Constructs a new Database based on the parsed spec.
  store(): Database {
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
  store: DatabaseSpec;
  name: string;

  // Returns a parsed spec, or null if the spec was invalid.
  static parse(spec: string): ?DatasetSpec {
    const match = spec.match(/^(.+)\:([a-zA-Z0-9\-_/]+)$/);
    if (!match) {
      return null;
    }
    const store = DatabaseSpec.parse(match[1]);
    if (!store) {
      return null;
    }
    return new this(store, match[2]);
  }

  constructor(store: DatabaseSpec, name: string) {
    this.store = store;
    this.name = name;
  }

  // Returns a new DataSet based on the parsed spec.
  set(): Dataset {
    return new Dataset(this.store.store(), this.name);
  }

  // Returns the value at the HEAD of this dataset, if any, or null otherwise.
  value(): Promise<any> {
    // Hm. Calling set() creates a Database that we then toss into the ether, which means we can't
    // call close() on it. Ideally, we'd fix that.
    return this.set().head()
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
  store: DatabaseSpec;
  hash: Hash;

  // Returns a parsed spec, or null if the spec was invalid.
  static parse(spec: string): ?HashSpec {
    const match = spec.match(/^(.+)\:(.+)$/);
    if (!match) {
      return null;
    }

    const hash = Hash.maybeParse(match[2]);
    if (!hash) {
      return null;
    }

    const store = DatabaseSpec.parse(match[1]);
    if (!store) {
      return null;
    }

    return new this(store, hash);
  }

  constructor(store: DatabaseSpec, hash: Hash) {
    this.store = store;
    this.hash = hash;
  }

  // Returns the value for the spec'd reference, if any, or null otherwise.
  value(): Promise<any> {
    const store = this.store.store();
    return store.readValue(this.hash).then(v => store.close().then(() => v));
  }
}

// Parses and returns the provided hash or dataset spec.
export function parseObjectSpec(spec: string): ?(DatasetSpec | HashSpec) {
  return HashSpec.parse(spec) || DatasetSpec.parse(spec);
}
