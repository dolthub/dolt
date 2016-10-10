// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import {
  AsyncIterator,
  equals,
  less,
  notNull,
} from '@attic/noms';
import type {AsyncIteratorResult} from '@attic/noms';
import type {
  PhotoSet,
  PhotoSetEntry,
  NomsPhoto,
} from './types.js';

type PhotoSetIteratorResult = AsyncIteratorResult<[number /* -date */, NomsPhoto]>;

export default class PhotoSetIterator extends AsyncIterator<[number, NomsPhoto]> {
}

export class SinglePhotoSetIterator extends PhotoSetIterator {
  _outer: AsyncIterator<PhotoSetEntry>;
  _start: ?NomsPhoto;
  _inner: ?AsyncIterator<NomsPhoto>;
  _negdate: ?number;

  static all(photoSet: PhotoSet): SinglePhotoSetIterator {
    return new SinglePhotoSetIterator(photoSet.iterator(), null);
  }

  static at(photoSet: PhotoSet, negdate: number, photo: ?NomsPhoto): SinglePhotoSetIterator {
    return new SinglePhotoSetIterator(photoSet.iteratorAt(negdate), photo);
  }

  constructor(outer: AsyncIterator<PhotoSetEntry>, start: ?NomsPhoto) {
    super();
    this._outer = outer;
    this._start = start;
  }

  async next(): Promise<PhotoSetIteratorResult> {
    if (!this._inner && await this._nextInner()) {
      return {done: true};
    }
    let next = await notNull(this._inner).next();
    if (next.done) {
      if (await this._nextInner()) {
        return {done: true};
      }
      next = await notNull(this._inner).next();
    }
    if (next.done) {
      return {done: true};
    }
    return {done: false, value: [notNull(this._negdate), next.value]};
  }

  async _nextInner(): Promise<boolean> {
    const {done, value} = await this._outer.next();
    if (done) {
      this._negdate = null;
      this._inner = null;
    } else {
      const [k, v] = notNull(value);
      this._negdate = k;
      this._inner = this._start ? v.iteratorAt(this._start) : v.iterator();
    }
    this._start = null;
    return done;
  }
}

export class PhotoSetIntersectionIterator extends PhotoSetIterator {
  _sets: PhotoSet[];
  _iters: AsyncIterator<[number, NomsPhoto]>[];
  _nexts: Promise<PhotoSetIteratorResult>[];

  constructor(sets: PhotoSet[], negdate: number) {
    super();
    this._sets = sets;
    this._iters = sets.map(s => SinglePhotoSetIterator.at(s, negdate, null));
    this._nexts = this._iters.map(iter => iter.next());
  }

  async next(): Promise<PhotoSetIteratorResult> {
    for (;;) {
      const all = await Promise.all(this._nexts);
      const values = all.filter(v => !v.done).map(v => notNull(v.value));
      if (values.length < all.length) {
        break;
      }

      let didIntersect = true, last = 0;
      for (let i = 1; i < values.length; i++) {
        if (!equals(values[i][1], values[0][1])) {
          didIntersect = false;
          if (!isIterationOrder(values[i][0], values[i][1],
                                values[last][0], values[last][1])) {
            last = i;
          }
        }
      }

      if (didIntersect) {
        this._nexts = this._iters.map(iter => iter.next());
        return {done: false, value: values[0]};
      }

      for (let i = 0; i < this._sets.length; i++) {
        if (i !== last) {
          const [negdate, photoRef] = values[last];
          this._iters[i] = SinglePhotoSetIterator.at(this._sets[i], negdate, photoRef);
          this._nexts[i] = this._iters[i].next();
        }
      }
    }

    return {done: true};
  }
}

export class EmptyIterator extends PhotoSetIterator {
  next(): Promise<PhotoSetIteratorResult> {
    return Promise.resolve({done: true});
  }
}

function isIterationOrder(d1: number, p1: NomsPhoto, d2: number, p2: NomsPhoto): boolean {
  // If the photo timestamps are different they will be in different sets, which are sorted by
  // reverse timestamp relative to each other. If they are equal they will be in the same inner
  // set, which are internally sorted by the photo's hash.
  return d1 === d2 ? less(p1, p2) : (d1 < d2);
}
