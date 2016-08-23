// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

export type AsyncIteratorResult<T> = {
  done: true,
  value?: void, // It would be better to leave out |value| entirely, but Flow doesn't like it.
} | {
  done: false,
  value: T,
};

export class AsyncIterator<T> {
  next(): Promise<AsyncIteratorResult<T>> {
    throw new Error('override');
  }
  return(): Promise<AsyncIteratorResult<T>> {
    throw new Error('override');
  }
}
