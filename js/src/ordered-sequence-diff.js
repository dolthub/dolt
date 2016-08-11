// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {invariant} from './assert.js';
import {OrderedSequence, OrderedSequenceCursor} from './ordered-sequence.js';
import {SequenceCursor} from './sequence.js';
import type Value from './value.js'; // eslint-disable-line no-unused-vars

// TODO: Expose an iteration API.

/**
 * Returns a 3-tuple [added, removed, modified] sorted keys.
 */
export default async function diff<K: Value, T>(
    last: OrderedSequence<K, T>, current: OrderedSequence<K, T>):
    Promise<[Array<K>, Array<K>, Array<K>]> {
  // TODO: Construct the cursor at exactly the right position. There is no point reading in the
  // first chunk of each sequence if we're not going to use them. This needs for chunks (or at
  // least meta chunks) to encode their height.
  // See https://github.com/attic-labs/noms/issues/1219.
  const [lastCur, currentCur] = await Promise.all([last.newCursorAt(), current.newCursorAt()]);
  const [added, removed, modified] = [[], [], []];

  while (lastCur.valid && currentCur.valid) {
    await fastForward(lastCur, currentCur);

    while (lastCur.valid && currentCur.valid &&
           !lastCur.sequence.getCompareFn(currentCur.sequence)(lastCur.idx, currentCur.idx)) {
      const lastKey = lastCur.getCurrentKey(), currentKey = currentCur.getCurrentKey();
      const compare = currentKey.compare(lastKey);
      if (compare < 0) {
        added.push(currentKey.value());
        await currentCur.advance();
      } else if (compare > 0) {
        removed.push(lastKey.value());
        await lastCur.advance();
      } else {
        modified.push(currentKey.value());
        await Promise.all([lastCur.advance(), currentCur.advance()]);
      }
    }
  }

  for (; lastCur.valid; await lastCur.advance()) {
    removed.push(lastCur.getCurrentKey().value());
  }
  for (; currentCur.valid; await currentCur.advance()) {
    added.push(currentCur.getCurrentKey().value());
  }

  return [added, removed, modified];
}

/**
 * Advances |a| and |b| past their common sequence of equal values.
 */
export function fastForward(a: OrderedSequenceCursor<any, any>, b: OrderedSequenceCursor<any, any>):
    Promise<void> {
  return a.valid && b.valid ? doFastForward(true, a, b).then() : Promise.resolve();
}

/*
 * Returns an array matching |a| and |b| respectively to whether that cursor has more values.
 */
async function doFastForward(allowPastEnd: boolean, a: OrderedSequenceCursor<any, any>,
    b: OrderedSequenceCursor<any, any>): Promise<[boolean, boolean]> {
  invariant(a.valid && b.valid);
  let aHasMore = true, bHasMore = true;

  while (aHasMore && bHasMore && isCurrentEqual(a, b)) {
    const aParent = a.parent, bParent = b.parent;

    if (aParent && bParent && isCurrentEqual(aParent, bParent)) {
      // Key optimisation: if the sequences have common parents, then entire chunks can be
      // fast-forwarded without reading unnecessary data.
      invariant(aParent instanceof OrderedSequenceCursor);
      invariant(bParent instanceof OrderedSequenceCursor);
      [aHasMore, bHasMore] = await doFastForward(false, aParent, bParent);

      const syncWithIdx = (cur, hasMore) => cur.sync().then(() => {
        if (hasMore) {
          cur.idx = 0;
        } else if (allowPastEnd) {
          cur.idx = cur.length;
        } else {
          cur.idx = cur.length - 1;
        }
      });
      await Promise.all([syncWithIdx(a, aHasMore), syncWithIdx(b, bHasMore)]);
    } else {
      if (a.canAdvanceLocal() && b.canAdvanceLocal()) {
        // Performance optimisation: allowing non-async resolution of leaf elements
        aHasMore = a.advanceLocal(allowPastEnd);
        bHasMore = b.advanceLocal(allowPastEnd);
      } else {
        await Promise.all([a.advance(allowPastEnd), b.advance(allowPastEnd)]).then(([am, bm]) => {
          aHasMore = am;
          bHasMore = bm;
        });
      }
    }
  }

  return [aHasMore, bHasMore];
}

function isCurrentEqual(a: SequenceCursor<any, any>, b: SequenceCursor<any, any>): boolean {
  return a.sequence.getCompareFn(b.sequence)(a.idx, b.idx);
}
