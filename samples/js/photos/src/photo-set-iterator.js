// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {
  AsyncIterator,
  equals,
  invariant,
  less,
  notNull,
} from '@attic/noms';
import type {AsyncIteratorResult} from '@attic/noms';
import type {
  PhotoSet,
  PhotoSetEntry,
  NomsPhoto,
} from './types.js';

type IterValue = [number /* -date */, NomsPhoto];

export default class PhotoSetIterator extends AsyncIterator<IterValue> {
}

export class SinglePhotoSetIterator extends PhotoSetIterator {
  _outer: PhotoSet;
  _startDate: number;
  _startPhoto: ?NomsPhoto;
  _outerIter: ?AsyncIterator<PhotoSetEntry>; // null until first call to next()
  _outerDate: number;                        // NaN until first call to next()
  _innerIter: ?AsyncIterator<NomsPhoto>;     // null until first call to next()
  _done: boolean;
  _lock: boolean;

  static all(photoSet: PhotoSet): SinglePhotoSetIterator {
    return new SinglePhotoSetIterator(photoSet, -Infinity, null);
  }

  static at(photoSet: PhotoSet, startDate: number, startPhoto: ?NomsPhoto): SinglePhotoSetIterator {
    return new SinglePhotoSetIterator(photoSet, startDate, startPhoto);
  }

  constructor(outer: PhotoSet, startDate: number, startPhoto: ?NomsPhoto) {
    super();
    this._outer = outer;
    this._startDate = startDate;
    this._startPhoto = startPhoto;
    this._outerIter = null;
    this._outerDate = NaN;
    this._innerIter = null;
    this._done = false;
    this._lock = false;
  }

  async next(): Promise<AsyncIteratorResult<IterValue>> {
    invariant(!this._lock);
    this._lock = true;
    try {
      return await this._next(); // must await so that finally will execute afterwards
    } finally {
      this._lock = false;
    }
  }

  async _next(): Promise<AsyncIteratorResult<IterValue>> {
    if (this._done) {
      return {done: true};
    }

    if (!this._outerIter) {
      // First call to next().
      // The outer iterator starts at startDate, which may be -Infinity. If outer iteration started
      // exactly at startDate, as opposed to somewhere afterwards, the inner iterator starts at
      // startPhoto. If not, startPhoto is irreleveant.
      this._outerIter = this._outer.iteratorAt(this._startDate);
      if (await this._outer.has(this._startDate)) {
        this._done = await this._nextInnerIter(this._startPhoto);
      } else {
        this._done = await this._nextInnerIter();
      }

      if (this._done) {
        return {done: true};
      }
    }

    // Get the next photo from the inner set.
    let next = await notNull(this._innerIter).next();
    if (!next.done) {
      return {
        done: false,
        value: [this._outerDate, next.value],
      };
    }

    // No more photos in the inner set, advance outer to next inner set.
    if (await this._nextInnerIter()) {
      // No more photos in the outer set, we're done.
      this._done = true;
      return {done: true};
    }

    // Get the first photo from the next inner set.
    next = await notNull(this._innerIter).next();
    invariant(!next.done);
    return {
      done: false,
      value: [this._outerDate, next.value],
    };
  }

  async _nextInnerIter(startPhoto: ?NomsPhoto = null): Promise<boolean> {
    const outerNext = await notNull(this._outerIter).next();
    if (outerNext.done) {
      return true;
    }

    const innerSet = outerNext.value[1];
    invariant(innerSet.size > 0);

    this._outerDate = outerNext.value[0];
    this._innerIter = startPhoto ? innerSet.iteratorAt(startPhoto) : innerSet.iterator();
    return false;
  }

  async return(): Promise<AsyncIteratorResult<IterValue>> {
    this._done = true;
    if (this._innerIter) {
      await this._innerIter.return();
    }
    if (this._outerIter) {
      await this._outerIter.return();
    }
    return {done: true};
  }
}

export class PhotoSetIntersectionIterator extends PhotoSetIterator {
  _sets: PhotoSet[];
  _iters: AsyncIterator<[number, NomsPhoto]>[];
  _nexts: Promise<AsyncIteratorResult<IterValue>>[];

  constructor(sets: PhotoSet[], startDate: number) {
    super();
    this._sets = sets;
    this._iters = sets.map(s => SinglePhotoSetIterator.at(s, startDate, null));
    this._nexts = this._iters.map(iter => iter.next());
  }

  async next(): Promise<AsyncIteratorResult<IterValue>> {
    for (;;) {
      const nexts = await Promise.all(this._nexts);
      const values = nexts.filter(v => !v.done).map(v => notNull(v.value));
      if (values.length < nexts.length) {
        break;
      }

      let didIntersect = true;
      let leastRecentIdx = 0;

      for (let i = 1; i < values.length; i++) {
        if (!equals(values[i][1], values[0][1])) {
          didIntersect = false;

          if (!isIterationOrder(values[i], values[leastRecentIdx])) {
            leastRecentIdx = i;
          }
        }
      }

      if (didIntersect) {
        // Cursors intersected, advance all iterators in lock step.
        this._nexts = this._iters.map(iter => iter.next());
        return {done: false, value: values[0]};
      }

      // Cursors did not intersect, advance all cursors up to or past the least recent.
      const [lrDate, lrPhoto] = values[leastRecentIdx];
      for (let i = 0; i < this._sets.length; i++) {
        if (i !== leastRecentIdx) {
          this._iters[i] = SinglePhotoSetIterator.at(this._sets[i], lrDate, lrPhoto);
          this._nexts[i] = this._iters[i].next();
        }
      }
    }

    return {done: true};
  }
}

export class EmptyIterator extends PhotoSetIterator {
  next(): Promise<AsyncIteratorResult<IterValue>> {
    return Promise.resolve({done: true});
  }
  return(): Promise<AsyncIteratorResult<IterValue>> {
    return Promise.resolve({done: true});
  }
}

function isIterationOrder(r1: IterValue, r2: IterValue): boolean {
  // If the photo timestamps are different they will be in different sets, which are sorted by
  // reverse timestamp relative to each other. If they are equal they will be in the same inner
  // set, which are internally sorted by the photo's hash.
  return r1[0] === r2[0] ? less(r1[1], r2[1]) : (r1[0] < r2[0]);
}
