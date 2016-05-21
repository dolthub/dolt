// @flow

import type {Splice} from './edit-distance.js';
import {calcSplices, SPLICE_ADDED, SPLICE_AT, SPLICE_FROM,
  SPLICE_REMOVED} from './edit-distance.js';
import {equals} from './compare.js';
import {IndexedMetaSequence} from './meta-sequence.js';
import {invariant} from './assert.js';
import type {IndexedSequence} from './indexed-sequence.js';

type LoadLimit = {
  count: number,
}

export function diff(last: IndexedSequence, lastHeight: number, lastOffset: number,
                     current: IndexedSequence, currentHeight: number, currentOffset: number,
                     loadLimit: ?LoadLimit): Promise<Array<Splice>> {

  const maybeLoadCompositeSequence = (ms: IndexedMetaSequence, idx: number, length: number) => {
    if (loadLimit) {
      loadLimit.count -= length;
      if (loadLimit.count < 0) {
        return Promise.reject(new Error('Load limit exceeded'));
      }
    }

    return ms.getCompositeChildSequence(idx, length);
  };

  if (lastHeight > currentHeight) {
    invariant(lastOffset === 0 && currentOffset === 0);
    invariant(last instanceof IndexedMetaSequence);
    return maybeLoadCompositeSequence(last, 0, last.length).then(lastChild =>
        diff(lastChild, lastHeight - 1, lastOffset, current, currentHeight, currentOffset,
             loadLimit));
  }

  if (currentHeight > lastHeight) {
    invariant(lastOffset === 0 && currentOffset === 0);
    invariant(current instanceof IndexedMetaSequence);
    return maybeLoadCompositeSequence(current, 0, current.length).then(currentChild =>
        diff(last, lastHeight, lastOffset, currentChild, currentHeight - 1, currentOffset,
             loadLimit));
  }

  invariant(last.isMeta === current.isMeta);
  invariant(lastHeight === currentHeight);

  const splices = calcSplices(last.length, current.length, last.isMeta ?
        (l, c) => equals(last.items[l].refValue, current.items[c].refValue) :
        (l, c) => equals(last.items[l], current.items[c]));

  const splicesP = splices.map(splice => {
    if (!last.isMeta || splice[SPLICE_REMOVED] === 0 || splice[SPLICE_ADDED] === 0) {
      splice[SPLICE_AT] += lastOffset;
      if (splice[SPLICE_ADDED] > 0) {
        splice[SPLICE_FROM] += currentOffset;
      }

      return [splice];
    }

    invariant(last instanceof IndexedMetaSequence && current instanceof IndexedMetaSequence);
    const lastChildP = maybeLoadCompositeSequence(last, splice[SPLICE_AT], splice[SPLICE_REMOVED]);
    const currentChildP = maybeLoadCompositeSequence(current, splice[SPLICE_FROM],
                                                     splice[SPLICE_ADDED]);

    let lastChildOffset = lastOffset;
    if (splice[SPLICE_AT] > 0) {
      lastChildOffset += last.getOffset(splice[SPLICE_AT] - 1) + 1;
    }
    let currentChildOffset = currentOffset;
    if (splice[SPLICE_FROM] > 0) {
      currentChildOffset += current.getOffset(splice[SPLICE_FROM] - 1) + 1;
    }

    return Promise.all([lastChildP, currentChildP]).then(childSequences =>
      diff(childSequences[0], lastHeight - 1, lastChildOffset, childSequences[1], currentHeight - 1,
           currentChildOffset,
           loadLimit));
  });

  return Promise.all(splicesP).then(spliceArrays => {
    const splices = [];
    for (let i = 0; i < spliceArrays.length; i++) {
      splices.push(...spliceArrays[i]);
    }
    return splices;
  });
}
