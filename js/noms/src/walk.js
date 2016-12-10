// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import type Value from './value.js';
import type {ValueReader} from './value-store.js';

export type WalkCallback = (v: Value) => ?boolean | Promise<?boolean>;

/**
 * Invokes `cb` once for `v` and each of its descendants. The returned `Promise` is resolved when
 * all invocations to `cb` have been resolved.
 *
 * The return value of `cb` indicates whether to recurse further into the tree. Return false or
 * `Promise.resolve(false)` to continue recursing. Return `true` or `Promise.resolve(true)` to skip
 * this node's children.
 *
 * If `cb` returns undefined or `Promise.resolve()`, the default is to continue recursing (`false`).
 */
export default async function walk(v: Value, vr: ValueReader, cb: WalkCallback): Promise<void> {
  let skip = cb(v);
  if (skip && skip !== true) {
    // Might be a Promise, but we can't check instanceof: https://phabricator.babeljs.io/T7340.
    skip = await skip;
  }

  if (skip) {
    return;
  }

  switch (typeof v) {
    case 'boolean':
    case 'number':
    case 'string':
      return;
  }

  return v.walkValues(vr, cb);
}
