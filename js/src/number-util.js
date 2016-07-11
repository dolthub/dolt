// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// convert float to [int, exp] where f = i / 2^exp
export function floatToIntExp(f: number): [number, number] {
  if (f === 0) {
    return [0, 0];
  }
  let exp = 0;
  while (!Number.isInteger(f)) {
    f *= 2;
    exp++;
  }
  return [f, exp];
}

// returns float value of i / 2^exp
export function intExpToFloat(i: number, exp: number): number {
  if (exp === 0) {
    return i;
  }
  return i / Math.pow(2, exp);
}
