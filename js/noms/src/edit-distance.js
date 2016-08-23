// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// This is a port (to flowtype, with minor modifications) of
// https://github.com/Polymer/observe-js/blob/master/src/observe.js#L1309.

export const DEFAULT_MAX_SPLICE_MATRIX_SIZE = 2e7;

export const SPLICE_UNASSIGNED = -1;
export const SPLICE_AT = 0;
export const SPLICE_REMOVED = 1;
export const SPLICE_ADDED = 2;
export const SPLICE_FROM = 3;

// Read a Splice as "at SpAt (in the previous state), SpRemoved elements were removed and SpAdded
// elements were inserted, which can be found starting at SpFrom in the current state"
type SpAt = number;
type SpRemoved = number;
type SpAdded = number;
type SpFrom = number;
export type Splice = [SpAt, SpRemoved, SpAdded, SpFrom];

export type EqualsFn = (prevIndex: number, currentIndex: number) => boolean;

const UNCHANGED = 0;
const UPDATED = 1;
const INSERTED = 2;
const REMOVED = 3;

export function calcSplices(previousLength: number, currentLength: number,
                            maxSpliceMatrixSize: number,
                            eqFn: EqualsFn): Array<Splice> {
  const minLength = Math.min(previousLength, currentLength);
  const prefixCount = sharedPrefix(eqFn, minLength);
  const suffixCount = sharedSuffix(eqFn, previousLength, currentLength, minLength - prefixCount);

  const previousStart = prefixCount;
  const currentStart = prefixCount;
  const previousEnd = previousLength - suffixCount;
  const currentEnd = currentLength - suffixCount;

  if (currentEnd - currentStart === 0 && previousEnd - previousStart === 0) {
    return [];
  }

  let splice;
  if (currentStart === currentEnd) {
    return [[previousStart, previousEnd - previousStart, 0, 0]];
  } else if (previousStart === previousEnd) {
    return [[previousStart, 0, currentEnd - currentStart, currentStart]];
  }

  previousLength = previousEnd - previousStart;
  currentLength = currentEnd - currentStart;

  if (previousLength * currentLength > maxSpliceMatrixSize) {
    return [[0, previousLength, currentLength, 0]];
  }

  const distances = calcEditDistances(eqFn, previousStart, previousLength, currentStart,
    currentLength);
  const ops = operationsFromEditDistances(distances);

  const splices = [];
  const addSplice = (s) => {
    if (s[SPLICE_FROM] === SPLICE_UNASSIGNED) {
      s[SPLICE_FROM] = 0;
    }
    splices.push(s);
    splice = undefined;
  };

  let index = currentStart;
  let previousIndex = previousStart;
  for (let i = 0; i < ops.length; i++) {
    switch (ops[i]) {
      case UNCHANGED:
        if (splice) {
          addSplice(splice);
        }

        index++;
        previousIndex++;
        break;
      case UPDATED:
        if (!splice) {
          splice = [index, 0, 0, SPLICE_UNASSIGNED];
        }

        if (splice[SPLICE_FROM] === SPLICE_UNASSIGNED) {
          splice[SPLICE_FROM] = previousIndex;
        }

        splice[SPLICE_REMOVED]++;
        splice[SPLICE_ADDED]++;

        index++;
        previousIndex++;
        break;
      case INSERTED:
        if (!splice) {
          splice = [index, 0, 0, SPLICE_UNASSIGNED];
        }

        splice[SPLICE_ADDED]++;
        if (splice[SPLICE_FROM] === SPLICE_UNASSIGNED) {
          splice[SPLICE_FROM] = previousIndex;
        }

        previousIndex++;
        break;
      case REMOVED:
        if (!splice) {
          splice = [index, 0, 0, SPLICE_UNASSIGNED];
        }

        splice[SPLICE_REMOVED]++;
        index++;
        break;
    }
  }

  if (splice) {
    addSplice(splice);
  }

  return splices;
}

function calcEditDistances(eqFn: EqualsFn, previousStart: number, previousLength: number,
                           currentStart: number,
                           currentLength: number): Array<Array<number>> {
  // "Deletion" columns
  const rowCount = previousLength + 1;
  const columnCount = currentLength + 1;
  const distances: Array<Array<number>> = new Array(rowCount);

  let i: number = 0;
  let j: number = 0;

  // "Addition" rows. Initialize null column.
  for (i = 0; i < rowCount; i++) {
    distances[i] = new Array(columnCount);
    distances[i][0] = i;
  }

  // Initialize null row
  for (j = 0; j < columnCount; j++) {
    distances[0][j] = j;
  }

  for (i = 1; i < rowCount; i++) {
    for (j = 1; j < columnCount; j++) {
      if (eqFn(previousStart + i - 1, currentStart + j - 1)) {
        distances[i][j] = distances[i - 1][j - 1];
      } else {
        const north = distances[i - 1][j] + 1;
        const west = distances[i][j - 1] + 1;
        distances[i][j] = north < west ? north : west;
      }
    }
  }

  return distances;
}

function operationsFromEditDistances(distances: Array<Array<number>>): Array<number> {
  let i = distances.length - 1;
  let j = distances[0].length - 1;
  let current = distances[i][j];
  const edits = [];
  while (i > 0 || j > 0) {
    if (i === 0) {
      edits.push(INSERTED);
      j--;
      continue;
    }
    if (j === 0) {
      edits.push(REMOVED);
      i--;
      continue;
    }
    const northWest = distances[i - 1][j - 1];
    const west = distances[i - 1][j];
    const north = distances[i][j - 1];

    let min;
    if (west < north)
      min = west < northWest ? west : northWest;
    else
      min = north < northWest ? north : northWest;

    if (min === northWest) {
      if (northWest === current) {
        edits.push(UNCHANGED);
      } else {
        edits.push(UPDATED);
        current = northWest;
      }
      i--;
      j--;
    } else if (min === west) {
      edits.push(REMOVED);
      i--;
      current = west;
    } else {
      edits.push(INSERTED);
      j--;
      current = north;
    }
  }

  edits.reverse();
  return edits;
}

function sharedPrefix(eqFn: EqualsFn, searchLength: number): number {
  for (let i = 0; i < searchLength; i++) {
    if (!eqFn(i, i)) {
      return i;
    }
  }

  return searchLength;
}

function sharedSuffix(eqFn: EqualsFn, previousLength: number, currentLength: number,
    searchLength: number): number {
  let count = 0;
  while (count < searchLength && eqFn(--previousLength, --currentLength)) {
    count++;
  }

  return count;
}
