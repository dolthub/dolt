// @flow

import type {Splice} from './edit-distance.js';
import {AsyncIterator} from './async-iterator.js';
import type {AsyncIteratorResult} from './async-iterator.js';
import {calcSplices, SPLICE_ADDED, SPLICE_AT, SPLICE_FROM,
  SPLICE_REMOVED} from './edit-distance.js';
import {equals} from './value.js';
import {IndexedMetaSequence} from './meta-sequence.js';
import {notNull, invariant} from './assert.js';
import {search, Sequence, SequenceCursor} from './sequence.js';

export class IndexedSequence<T> extends Sequence<T> {
  getOffset(idx: number): number { // eslint-disable-line no-unused-vars
    throw new Error('override');
  }

  async newCursorAt(idx: number): Promise<IndexedSequenceCursor> {
    let cursor: ?IndexedSequenceCursor = null;
    let sequence: ?IndexedSequence = this;

    while (sequence) {
      cursor = new IndexedSequenceCursor(cursor, sequence, 0);
      idx -= cursor.advanceToOffset(idx);
      sequence = await cursor.getChildSequence();
    }

    return notNull(cursor);
  }

  range(start: number, end: number): Promise<Array<T>> { // eslint-disable-line no-unused-vars
    throw new Error('override');
  }
}

export class IndexedSequenceCursor<T> extends SequenceCursor<T, IndexedSequence> {
  advanceToOffset(idx: number): number {
    this.idx = search(this.length, (i: number) => idx <= this.sequence.getOffset(i));

    if (this.sequence.isMeta && this.idx === this.length) {
      this.idx = this.length - 1;
    }

    return this.idx > 0 ? this.sequence.getOffset(this.idx - 1) + 1 : 0;
  }

  clone(): IndexedSequenceCursor<T> {
    return new IndexedSequenceCursor(this.parent && this.parent.clone(), this.sequence, this.idx);
  }
}

export class IndexedSequenceIterator<T> extends AsyncIterator<T> {
  _iterator: Promise<AsyncIterator<T>>;

  constructor(cursorP: Promise<IndexedSequenceCursor<T>>) {
    super();
    this._iterator = cursorP.then(cur => cur.iterator());
  }

  next(): Promise<AsyncIteratorResult<T>> {
    return this._iterator.then(it => it.next());
  }

  return(): Promise<AsyncIteratorResult<T>> {
    return this._iterator.then(it => it.return());
  }
}

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
        (l, c) => last.items[l].ref.equals(current.items[c].ref) :
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
