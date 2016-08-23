// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import type Value from './value.js';

// All Noms values are ordered. The ordering is booleans, numbers, strings and then Values.
// All Value objects are ordered by their hash.

export function compare(v1: Value, v2: Value): number {
  const t1 = typeof v1;
  const t2 = typeof v2;

  switch (t1) {
    case 'boolean':
      if (t2 === 'boolean') {
        return Number(v1) - Number(v2);
      }
      return -1;

    case 'number':
      switch (t2) {
        case 'boolean':
          return 1;
        case 'number':
          // $FlowIssue: Flow does not realize both v1 and v2 are numbers here.
          return v1 - v2;
      }
      return -1;

    case 'string':
      switch (t2) {
        case 'boolean':
        case 'number':
          return 1;
        case 'string':
          // $FlowIssue: Flow does not realize both v1 and v2 are strings here.
          return v1 === v2 ? 0 : v1 < v2 ? -1 : 1;
      }
      return -1;

    case 'object': {
      switch (t2) {
        case 'boolean':
        case 'number':
        case 'string':
          return 1;
      }

      // $FlowIssue: Flow does not realize that v1 and v2 are Values here.
      return v1.hash.compare(v2.hash);
    }
    default:
      throw new Error('unreachable');
  }
}

export function less(v1: Value, v2: Value): boolean {
  return compare(v1, v2) < 0;
}

export function equals(v1: Value, v2: Value): boolean {
  return compare(v1, v2) === 0;
}
