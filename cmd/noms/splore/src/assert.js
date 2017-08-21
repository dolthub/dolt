// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

export function notNull<T>(v: ?T): T {
  if (v !== null && v !== undefined) {
    return v;
  }
  throw new Error('Non-null assertion failed');
}
