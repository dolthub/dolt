// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

/**
 * Searches between 0 and `length` until the compare function returns 0 (equal). If no match is
 * found then this returns `length`.
 */
export default function search(length: number, compare: (i: number) => number): number {
  // f = i => compare(i) >= 0
  // Define f(-1) == false and f(n) == true.
  // Invariant: f(i-1) == false, f(j) == true.
  let lo = 0;
  let hi = length;
  while (lo < hi) {
    const h = lo + (((hi - lo) / 2) | 0); // avoid overflow when computing h
    const c = compare(h);
    if (c === 0) {
      return h;
    }
    // i ≤ h < j
    if (c < 0) {
      lo = h + 1; // preserves f(i-1) == false
    } else {
      hi = h; // preserves f(j) == true
    }
  }

  // i == j, f(i-1) == false, and f(j) (= f(i)) == true  =>  answer is i.
  return lo;
}
