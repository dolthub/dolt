// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

export type AsyncIterator<T> = {
  next(): Promise<AsyncIteratorResult<T>>,
  return(): Promise<AsyncIteratorResult<T>>,
};

export type AsyncIteratorResult<T> = {
  done: true,
} | {
  done: false,
  value: T,
};
