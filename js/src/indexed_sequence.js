// @flow

import type {Splice} from './edit_distance.js';
import {AsyncIterator, AsyncIteratorResult} from './async_iterator.js';
import {calcSplices, SPLICE_ADDED, SPLICE_AT, SPLICE_FROM,
  SPLICE_REMOVED} from './edit_distance.js';
import {equals} from './value.js';
import {IndexedMetaSequence} from './meta_sequence.js';
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
  _cursorP: Promise<IndexedSequenceCursor<T>>;
  _iterator: ?AsyncIterator<T>;

  constructor(cursorP: Promise<IndexedSequenceCursor<T>>) {
    super();
    this._cursorP = cursorP;
  }

  async _ensureIterator(): Promise<AsyncIterator<T>> {
    if (!this._iterator) {
      this._iterator = (await this._cursorP).iterator();
    }
    return this._iterator;
  }

  next(): Promise<AsyncIteratorResult<T>> {
    return this._ensureIterator().then(it => it.next());
  }

  return(): Promise<AsyncIteratorResult<T>> {
    return this._ensureIterator().then(it => it.return());
  }
}

export function diff(last: IndexedSequence, lastHeight: number, lastOffset: number,
                     current: IndexedSequence, currentHeight: number, currentOffset: number):
    Promise<Array<Splice>> {
  if (lastHeight > currentHeight) {
    invariant(lastOffset === 0 && currentOffset === 0);
    invariant(last instanceof IndexedMetaSequence);
    return last.getCompositeChildSequence(0, last.length).then(lastChild =>
        diff(lastChild, lastHeight - 1, lastOffset, current, currentHeight, currentOffset));
  }

  if (currentHeight > lastHeight) {
    invariant(lastOffset === 0 && currentOffset === 0);
    invariant(current instanceof IndexedMetaSequence);
    return current.getCompositeChildSequence(0, current.length).then(currentChild =>
        diff(last, lastHeight, lastOffset, currentChild, currentHeight - 1, currentOffset));
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
    const lastChildP = last.getCompositeChildSequence(splice[SPLICE_AT], splice[SPLICE_REMOVED]);
    const currentChildP = current.getCompositeChildSequence(splice[SPLICE_FROM],
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
           currentChildOffset));
  });

  return Promise.all(splicesP).then(spliceArrays => {
    const splices = [];
    for (let i = 0; i < spliceArrays.length; i++) {
      splices.push(...spliceArrays[i]);
    }
    return splices;
  });
}
