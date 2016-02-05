// @flow

// This is a port (to flowtype, with minor modifications) of
// https://github.com/Polymer/observe-js/blob/master/src/observe.js#L1309.

// const SPLICE_AT = 0;
const SPLICE_REMOVED = 1;
const SPLICE_ADDED = 2;
const SPLICE_FROM = 3;

// Read a Splice [SPLICE_AT, SPLICE_REMOVED, SPLICE_ADDED, SPLICE_FROM] as...
// "at SPLICE_AT (in the previous state), SPLICE_REMOVED elements were removed and SPLICE_ADDED
// elements were inserted, which can be found starting at SPLICE_FROM in the current state"
export type Splice = [number, number, number, number];

export type EqualsFn<T> = (a: T, b: T) => boolean;

const UNCHANGED = 0;
const UPDATED = 1;
const INSERTED = 2;
const REMOVED = 3;

export function calcSplices<T>(eqFn: EqualsFn, previous: Array<T>,
                               current: Array<T>): Array<Splice> {
  return calcSplicesAt(eqFn, previous, 0, previous.length, current, 0, current.length);
}

export function calcSplicesAt<T>(eqFn: EqualsFn, previous: Array<T>, previousStart: number,
                                 previousEnd: number,
                                 current: Array<T>,
                                 currentStart: number,
                                 currentEnd: number): Array<Splice> {
  let prefixCount = 0;
  let suffixCount = 0;

  const minLength = Math.min(currentEnd - currentStart, previousEnd - previousStart);
  if (currentStart === 0 && previousStart === 0) {
    prefixCount = sharedPrefix(eqFn, previous, current, minLength);
  }

  if (currentEnd === current.length && previousEnd === previous.length) {
    suffixCount = sharedSuffix(eqFn, previous, current, minLength - prefixCount);
  }

  currentStart += prefixCount;
  previousStart += prefixCount;
  currentEnd -= suffixCount;
  previousEnd -= suffixCount;

  if (currentEnd - currentStart === 0 && previousEnd - previousStart === 0) {
    return [];
  }

  let splice;
  if (currentStart === currentEnd) {
    return [[previousStart, previousEnd - previousStart, 0, 0]];
  } else if (previousStart === previousEnd) {
    return [[previousStart, 0, currentEnd - currentStart, currentStart]];
  }

  const distances = calcEditDistances(eqFn, previous, previousStart, previousEnd, current,
    currentStart, currentEnd);
  const ops = operationsFromEditDistances(distances);

  const splices = [];
  let index = currentStart;
  let previousIndex = previousStart;
  for (let i = 0; i < ops.length; i++) {
    switch (ops[i]) {
      case UNCHANGED:
        if (splice) {
          splices.push(splice);
          splice = undefined;
        }

        index++;
        previousIndex++;
        break;
      case UPDATED:
        if (!splice) {
          splice = [index, 0, 0, 0];
        }

        if (splice[SPLICE_FROM] === 0) {
          splice[SPLICE_FROM] = previousIndex;
        }

        splice[SPLICE_REMOVED]++;
        splice[SPLICE_ADDED]++;

        index++;
        previousIndex++;
        break;
      case INSERTED:
        if (!splice) {
          splice = [index, 0, 0, 0];
        }

        splice[SPLICE_ADDED]++;
        if (splice[SPLICE_FROM] === 0) {
          splice[SPLICE_FROM] = previousIndex;
        }

        previousIndex++;
        break;
      case REMOVED:
        if (!splice) {
          splice = [index, 0, 0, 0];
        }

        splice[SPLICE_REMOVED]++;
        index++;
        break;
    }
  }

  if (splice) {
    splices.push(splice);
  }

  return splices;
}

function calcEditDistances<T>(eqFn: EqualsFn, previous: Array<T>, previousStart: number,
                              previousEnd: number,
                              current: Array<T>,
                              currentStart: number,
                              currentEnd: number): Array<Array<number>> {
  // "Deletion" columns
  const rowCount = previousEnd - previousStart + 1;
  const columnCount = currentEnd - currentStart + 1;
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
      if (eqFn(current[currentStart + j - 1], previous[previousStart + i - 1])) {
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

function sharedPrefix<T>(eqFn: EqualsFn, previous: Array<T>, current: Array<T>,
    searchLength: number) {
  for (let i = 0; i < searchLength; i++) {
    if (!eqFn(current[i], previous[i])) {
      return i;
    }
  }

  return searchLength;
}

function sharedSuffix<T>(eqFn: EqualsFn, previous: Array<T>, current: Array<T>,
    searchLength: number) {
  let index1 = current.length;
  let index2 = previous.length;
  let count = 0;
  while (count < searchLength && eqFn(current[--index1], previous[--index2])) {
    count++;
  }

  return count;
}
