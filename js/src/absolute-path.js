// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {invariant} from './assert.js';
import {datasetRe} from './dataset.js';
import Database from './database.js';
import Hash, {stringLength} from './hash.js';
import Path from './path.js';
import type Value from './value.js';

const datasetCapturePrefixRe = new RegExp('^(' + datasetRe.source + ')');

/**
 * An AbsolutePath is a Path relative to either a dataset head, or a hash.
 *
 * E.g. in a spec like `http://demo.noms.io::foo.bar` this is the `foo.bar` component, or in
 * `http://demo.noms.io::#abcdef.bar` the `#abcdef.bar` component.
 *
 * See https://github.com/attic-labs/noms/blob/master/doc/spelling.md.
 */
export default class AbsolutePath {
  /** The dataset ID that `path` is in, or `''` if none. */
  dataset: string;

  /** The hash the that `path` is in, if any. */
  hash: Hash | null;

  /** Path relative to either `dataset` or `hash`. */
  path: Path;

  /**
   * Returns `str` parsed as an AbsolutePath. Throws a `SyntaxError` if `str` isn't a valid path.
   */
  static parse(str: string): AbsolutePath {
    if (str === '') {
      throw new SyntaxError('Empty path');
    }

    let dataset = '';
    let hash = null;
    let pathStr = '';

    if (str[0] === '#') {
      const tail = str.slice(1);
      if (tail.length < stringLength) {
        throw new SyntaxError(`Invalid hash: ${tail}`);
      }

      const hashStr = tail.slice(0, stringLength);
      hash = Hash.parse(hashStr);
      if (hash === null) {
        throw new SyntaxError(`Invalid hash: ${hashStr}`);
      }

      pathStr = tail.slice(stringLength);
    } else {
      const parts = datasetCapturePrefixRe.exec(str);
      if (!parts) {
        throw new SyntaxError(`Invalid dataset name: ${str}`);
      }

      invariant(parts.length === 2);
      dataset = parts[1];
      pathStr = str.slice(parts[0].length);
    }

    if (pathStr.length === 0) {
      return new AbsolutePath(dataset, hash, new Path());
    }

    const path = Path.parse(pathStr);
    return new AbsolutePath(dataset, hash, path);
  }

  constructor(dataset: string, hash: Hash | null, path: Path) {
    this.dataset = dataset;
    this.hash = hash;
    this.path = path;
  }

  async resolve(db: Database): Promise<Value | null> {
    let val = null;
    if (this.dataset !== '') {
      val = await db.head(this.dataset);
    } else if (this.hash !== null) {
      val = await db.readValue(this.hash);
    } else {
      throw new Error('unreachable');
    }

    if (val === undefined) {
      val = null;
    }
    return val === null ? null : this.path.resolve(val);
  }

  toString(): string {
    if (this.dataset !== '') {
      return this.dataset + this.path.toString();
    }
    if (this.hash !== null) {
      return '#' + this.hash.toString() + this.path.toString();
    }
    throw new Error('unreachable');
  }
}
