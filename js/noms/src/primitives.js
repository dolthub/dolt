// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

export type primitive =
    number |
    string |
    boolean;

export function isPrimitive(v: any): boolean {
  switch (typeof v) {
    case 'string':
    case 'number':
    case 'boolean':
      return true;
    default:
      return false;
  }
}
