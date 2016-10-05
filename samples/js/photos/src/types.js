// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import type {
  Map,
  Set,
} from '@attic/noms';
import {Struct} from '@attic/noms'; // eslint-disable-line no-unused-vars
import type {MapEntry} from '@attic/noms';

export type CountMap = Map<number, Set<string>>;

/**
 * PhotoSet is keyed by (-1 * the photo's Unix timestamp), so that iteration will be by recency
 * (reverse chronological order).
 */
export type PhotoSet = Map<number, Set<NomsPhoto>>;
export type PhotoSetEntry = MapEntry<number, Set<NomsPhoto>>;

declare class Face extends Struct {
  h: number;
  name: string,
  w: number;
  x: number;
  y: number;
}

declare class NomsPhoto extends Struct {
  faces: Set<Face>;
  sizes: Map<PhotoSize, string>;
}

declare class PhotoIndex extends Struct {
  byDate: PhotoSet;
  byFace: Map<string, PhotoSet>;
  byTag: Map<string, PhotoSet>;
  facesByCount: CountMap;
  tagsByCount: CountMap;
}

declare class PhotoSize extends Struct {
  height: number;
  setHeight: (h: number) => PhotoSize;
  width: number;
  setWidth: (w: number) => PhotoSize;
}

export type {Face, NomsPhoto, PhotoIndex, PhotoSize};
