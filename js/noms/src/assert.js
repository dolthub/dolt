// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

type Message = string | () => string;

// Asserts that exp is truthy. If it isn't then an exception is thrown. If computing the error
// message is expensive a callback can be used instead.
export function invariant(exp: any, message: Message = 'Invariant violated') {
  if (process.env.NODE_ENV === 'production') return;
  if (!exp) {
    throw new Error(typeof message !== 'string' ? message() : message);
  }
}

export function notNull<T>(v: ?T): T {
  invariant(v !== null && v !== undefined, 'Unexpected null value');
  return v;
}
