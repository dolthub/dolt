// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

function float64IsInt(f: number): boolean {
  return Math.trunc(f) === f;
}

/**
 * Converts a number to [int, exp] where f = i / 2^exp
 */
export function floatToIntExp(f: number): [number, number] {
  if (f === 0) {
    return [0, 0];
  }

  const sign = Math.sign(f);
  f = Math.abs(f);

  let exp = 0;

  // Really large float, bring down to max safe integer so that it can be correctly represented by
  // float64.
  while (f > Number.MAX_SAFE_INTEGER) {
    f /= 2;
    exp--;
  }

  while (!float64IsInt(f)) {
    f *= 2;
    exp++;
  }
  return [sign * f, exp];
}

/**
 * Computes the float value of i / 2^exp
 */
export function intExpToFloat(i: number, exp: number): number {
  if (exp === 0) {
    return i;
  }
  return i / Math.pow(2, exp);
}
