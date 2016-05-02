// @flow

import Dataset from './dataset.js';
import Database from './database.js';
import HttpStore from './http-store.js';
import MemoryStore from './memory-store.js';
import Ref from './ref.js';

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
  db(): Database {
    if (this.scheme === 'mem') {
      return new Database(new MemoryStore());
    }
    if (this.scheme === 'http') {
      return new Database(new HttpStore(`${this.scheme}:${this.path}`));
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
  db: DatabaseSpec;
  name: string;

  // Returns a parsed spec, or null if the spec was invalid.
  static parse(spec: string): ?DatasetSpec {
    const match = spec.match(/^(.+)\:(.+)$/);
    if (!match) {
      return null;
    }
    const db = DatabaseSpec.parse(match[1]);
    if (!db) {
      return null;
    }
    return new this(db, match[2]);
  }

  constructor(db: DatabaseSpec, name: string) {
    this.db = db;
    this.name = name;
  }

  // Returns a new DataSet based on the parsed spec.
  set(): Dataset {
    return new Dataset(this.db.db(), this.name);
  }

  // Returns the value at the HEAD of this dataset, if any, or null otherwise.
  value(): Promise<any> {
    return this.set().head()
      .then(commit => commit && commit.value);
  }
}


// A parsed specification for the location of a Noms ref.
// For example: 'mem:sha1-5ba4be791d336d3184be7ee7dc598037f410ef96' or
// 'https://ds.noms.io/aa/music:sha1-3ff6ee6add3490621a8886608cc8423dba3cf7ca'
//
// See "spelling objects" for details on supported syntaxes:
// https://docs.google.com/document/d/1QgKcRS304llwU0ECahKtn8lGBFmT5zXzWr-5tah1S_4/edit
export class RefSpec {
  db: DatabaseSpec;
  ref: Ref;

  // Returns a parsed spec, or null if the spec was invalid.
  static parse(spec: string): ?RefSpec {
    const match = spec.match(/^(.+)\:(.+)$/);
    if (!match) {
      return null;
    }

    const ref = Ref.maybeParse(match[2]);
    if (!ref) {
      return null;
    }

    const db = DatabaseSpec.parse(match[1]);
    if (!db) {
      return null;
    }

    return new this(db, ref);
  }

  constructor(db: DatabaseSpec, ref: Ref) {
    this.db = db;
    this.ref = ref;
  }

  // Returns the value for the spec'd reference, if any, or null otherwise.
  value(): Promise<any> {
    return this.db.db().readValue(this.ref);
  }
}

// Parses and returns the provided ref or dataset spec.
export function parseObjectSpec(spec: string): ?(DatasetSpec|RefSpec) {
  return RefSpec.parse(spec) || DatasetSpec.parse(spec);
}
