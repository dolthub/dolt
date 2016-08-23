// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Ported from golang code. Search uses binary search to find and return the smallest index i in
// [0, n) at which f(i) is true, assuming that on the range [0, n), f(i) == true implies
// f(i+1) == true.
export default function search(length: number, f: (i: number) => boolean): number {
  // f = i => compare(i) >= 0
  // Define f(-1) == false and f(n) == true.
  // Invariant: f(i-1) == false, f(j) == true.
  let lo = 0;
  let hi = length;
  while (lo < hi) {
    const h = lo + (((hi - lo) / 2) | 0); // avoid overflow when computing h
    // i ≤ h < j
    if (!f(h)) {
      lo = h + 1; // preserves f(i-1) == false
    } else {
      hi = h; // preserves f(j) == true
    }
  }

  // i == j, f(i-1) == false, and f(j) (= f(i)) == true  =>  answer is i.
  return lo;
}
