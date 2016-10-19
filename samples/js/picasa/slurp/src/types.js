// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

export type V<T> = {
  $t: T;
};

export type Api<T> = {
  feed: {
    entry: T[];
  };
};

// user Api

export type User = Api<UserEntry>;

export type UserEntry = {
  gphoto$id: V<string>;
  gphoto$numphotos: V<number>;
};

// albums Api

export type Albums = Api<AlbumsEntry>;

export type AlbumsEntry = {};

// faces Api

export type Faces = Api<FacesEntry>;

export type FacesEntry = {
  gphoto$albumid: V<string>;
  gphoto$shapes: Shape[];
};

export type Shape = {};
