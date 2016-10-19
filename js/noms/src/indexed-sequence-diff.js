// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import type {Splice} from './edit-distance.js';
import {calcSplices, SPLICE_ADDED, SPLICE_AT, SPLICE_FROM,
  SPLICE_REMOVED} from './edit-distance.js';
import {IndexedMetaSequence} from './meta-sequence.js';
import {invariant} from './assert.js';
import type {IndexedSequence} from './indexed-sequence.js';

export function diff(last: IndexedSequence<any>, lastHeight: number, lastOffset: number,
                     current: IndexedSequence<any>, currentHeight: number, currentOffset: number,
                     maxSpliceMatrixSize: number): Promise<Array<Splice>> {

  if (lastHeight > currentHeight) {
    invariant(lastOffset === 0 && currentOffset === 0);
    invariant(last instanceof IndexedMetaSequence);
    return last.getCompositeChildSequence(0, last.length).then(lastChild =>
        diff(lastChild, lastHeight - 1, lastOffset, current, currentHeight, currentOffset,
             maxSpliceMatrixSize));
  }

  if (currentHeight > lastHeight) {
    invariant(lastOffset === 0 && currentOffset === 0);
    invariant(current instanceof IndexedMetaSequence);
    return current.getCompositeChildSequence(0, current.length).then(currentChild =>
        diff(last, lastHeight, lastOffset, currentChild, currentHeight - 1, currentOffset,
             maxSpliceMatrixSize));
  }

  invariant(last.isMeta === current.isMeta);
  invariant(lastHeight === currentHeight);

  const splices = calcSplices(last.length, current.length, maxSpliceMatrixSize,
    last.getCompareFn(current));

  const splicesP = splices.map(splice => {
    if (!last.isMeta) {
      // This is a leaf sequence, we can just report the splice, but it's indices must be offset.
      splice[SPLICE_AT] += lastOffset;
      if (splice[SPLICE_ADDED] > 0) {
        splice[SPLICE_FROM] += currentOffset;
      }

      return [splice];
    }

    if (splice[SPLICE_REMOVED] === 0 || splice[SPLICE_ADDED] === 0) {
      // An entire subtree was removed at a meta level. We must do some math to map the splice from
      // the meta level into the leaf coordinates.
      let beginRemoveIndex = 0;
      if (splice[SPLICE_AT] > 0) {
        beginRemoveIndex = last.cumulativeNumberOfLeaves(splice[SPLICE_AT] - 1);
      }
      let endRemoveIndex = 0;
      if (splice[SPLICE_AT] + splice[SPLICE_REMOVED] > 0) {
        endRemoveIndex =
            last.cumulativeNumberOfLeaves(splice[SPLICE_AT] + splice[SPLICE_REMOVED] - 1);
      }
      let beginAddIndex = 0;
      if (splice[SPLICE_FROM] > 0) {
        beginAddIndex = current.cumulativeNumberOfLeaves(splice[SPLICE_FROM] - 1);
      }
      let endAddIndex = 0;
      if (splice[SPLICE_FROM] + splice[SPLICE_ADDED] > 0) {
        endAddIndex =
            current.cumulativeNumberOfLeaves(splice[SPLICE_FROM] + splice[SPLICE_ADDED] - 1);
      }

      splice[SPLICE_AT] = lastOffset + beginRemoveIndex;
      splice[SPLICE_REMOVED] = endRemoveIndex - beginRemoveIndex;

      splice[SPLICE_ADDED] = endAddIndex - beginAddIndex;
      if (splice[SPLICE_ADDED] > 0) {
        splice[SPLICE_FROM] = currentOffset + beginAddIndex;
      }

      return [splice];
    }

    // Meta sequence splice which includes removed & added sub-sequences. Must recurse down.
    invariant(last instanceof IndexedMetaSequence && current instanceof IndexedMetaSequence);
    const lastChildP = last.getCompositeChildSequence(splice[SPLICE_AT], splice[SPLICE_REMOVED]);
    const currentChildP = current.getCompositeChildSequence(splice[SPLICE_FROM],
      splice[SPLICE_ADDED]);

    let lastChildOffset = lastOffset;
    if (splice[SPLICE_AT] > 0) {
      lastChildOffset += last.cumulativeNumberOfLeaves(splice[SPLICE_AT] - 1);
    }
    let currentChildOffset = currentOffset;
    if (splice[SPLICE_FROM] > 0) {
      currentChildOffset += current.cumulativeNumberOfLeaves(splice[SPLICE_FROM] - 1);
    }

    return Promise.all([lastChildP, currentChildP]).then(childSequences =>
      diff(childSequences[0], lastHeight - 1, lastChildOffset, childSequences[1],
           currentHeight - 1,
           currentChildOffset,
           maxSpliceMatrixSize));
  });

  return Promise.all(splicesP).then(spliceArrays => {
    const splices = [];
    for (let i = 0; i < spliceArrays.length; i++) {
      splices.push(...spliceArrays[i]);
    }
    return splices;
  });
}
