// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import type Value from './value.js';
import {ValueBase} from './value.js';
import type {ValueReader} from './value-store.js';
import Ref from './ref.js';
import Collection from './collection.js';
import Hash from './hash.js';

export type WalkCallback = (v: Value) => ?boolean | Promise<?boolean>;

const maxRefCount = (1 << 12); // ~16MB of data

/**
 * Invokes `cb` once for `v` and each of its descendants. The returned `Promise` is resolved when
 * all invocations to `cb` have been resolved.
 *
 * The return value of `cb` indicates whether to recurse further into the tree. Return false or
 * `Promise.resolve(false)` to continue recursing. Return `true` or `Promise.resolve(true)` to skip
 * this node's children.
 *
 * If `cb` returns undefined or `Promise.resolve()`, the default is to continue recursing (`false`).
 *
 */

interface ValueRec {
  v: Value;
  needsCallback: boolean;
}

interface HashRec {
  h: Hash;
  needsCallback: boolean;
}

export default async function walk(v: Value, vr: ValueReader, cb: WalkCallback): Promise<void> {
  const values: ValueRec[] = [{v: v, needsCallback: true}];
  const refs: HashRec[] = [];

  const visited = Object.create(null);
  const childValuesP = [];
  const childCB = [];

  while (values.length > 0 || refs.length > 0) {
    // Visit all values located *within* loaded chunks.
    while (values.length > 0) {
      const rec = values.pop();
      if (rec.needsCallback) {
        let skip = cb(rec.v);
        if (skip && skip !== true) {
          skip = await skip;
        }
        if (skip) {
          continue;
        }
      }

      v = rec.v;
      if (v instanceof Ref) {
        refs.push({h: v.targetHash, needsCallback: true});
        continue;
      }

      if (v instanceof Collection && v.sequence.isMeta) {
        v.sequence.items.forEach(mt => {
          if (mt.child) {
            // Eagerly visit uncommitted child sequences.
            values.push({v: mt.child, needsCallback: false});
          } else {
            refs.push({h: mt.ref.targetHash, needsCallback: false});
          }
        });
        continue;
      }

      if (v instanceof ValueBase) {
        await v.walkValues(vr, v => {
          values.push({v: v, needsCallback: true});
          return;
        });
      }
    }

    // Load the next level of chunks.
    while (refs.length > 0 && childValuesP.length <= maxRefCount) {
      const rec = refs.pop();
      const hstr = rec.h.toString();
      if (visited[hstr]) {
        return;
      }

      visited[hstr] = true;
      childValuesP.push(vr.readValue(rec.h));
      childCB.push(rec.needsCallback);
    }

    (await Promise.all(childValuesP)).forEach(
        (child: Value, idx) => values.push({v: child, needsCallback: childCB[idx]}));

    childValuesP.length = 0;
    childCB.length = 0;
  }
}
