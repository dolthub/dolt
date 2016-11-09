// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import type {NomsPhoto, PhotoSize} from './types.js';

import type {MapEntry} from '@attic/noms';
import {
  StructMirror,
  equals,
  invariant,
  notNull,
} from '@attic/noms';
export type SizeEntry = MapEntry<PhotoSize, string>;

/**
 * createPhoto asynchronously derives a Photo instance from a NomsPhoto.
 */
export function createPhoto(path: string, p: NomsPhoto): Promise<Photo> {
  const sizeEntries = [];
  return p.sizes.forEach((url, size) => {
    sizeEntries.push([size, url]);
  }).then(() => new Photo(path, p, sizeEntries));
}

/**
 * Photo is a wrapper around the NomsPhoto struct.
 */
export default class Photo {
  path: string;
  nomsPhoto: NomsPhoto;

  _sizeEntries: SizeEntry[];

  constructor(path: string, nomsPhoto: NomsPhoto, sizeEntries: SizeEntry[]) {
    this.path = path;
    this.nomsPhoto = nomsPhoto;
    this._sizeEntries = sizeEntries;
  }

  equals(p: Photo): boolean {
    return equals(this.nomsPhoto, p.nomsPhoto);
  }

  /**
   * getBestSize gets the best photo that fits in dimensions (width, height),
   * where best is the smallest photo which is larger than the dimensions, or
   * failing that the widest - and one presumes therefore the largest.
   */
  getBestSize(width: number, height: number): SizeEntry {
    const bestWidth = this._smallestImageAtLeast('width', width);
    const bestHeight = this._smallestImageAtLeast('height', height);

    if (bestWidth === null && bestHeight === null) {
      return this._widestImage();
    }
    if (bestWidth === null) {
      return notNull(bestHeight);
    }
    if (bestHeight === null) {
      return bestWidth;
    }
    return bestWidth[0].width > bestHeight[0].width ? bestWidth : bestHeight;
  }

  _smallestImageAtLeast(axis: 'width' | 'height', atLeast: number): (SizeEntry | null) {
    const entries = this._sizeEntries;
    let smallest = null;
    let smallestValue = 0;
    for (const entry of entries) {
      const axisValue = new StructMirror(entry[0]).get(axis);
      invariant(typeof axisValue === 'number' && typeof smallestValue === 'number');
      if (axisValue >= atLeast && (smallest === null || axisValue < smallestValue)) {
        smallest = entry;
        smallestValue = axisValue;
      }
    }
    return smallest;
  }

  _widestImage(): SizeEntry {
    const entries = this._sizeEntries;
    let widest = entries[0];
    for (let i = 1; i < entries.length; i++) {
      const entry = entries[i];
      if (entry[0].width > widest[0].width) {
        widest = entry;
      }
    }
    return widest;
  }
}
