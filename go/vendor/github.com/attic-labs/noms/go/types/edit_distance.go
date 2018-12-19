// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

// ported from edit-distance.js, itself a port with minor modifications of
// https://github.com/Polymer/observe-js/blob/master/src/observe.js#L1309.

import (
	"fmt"
	"math"
)

const (
	DEFAULT_MAX_SPLICE_MATRIX_SIZE = 2e7
	SPLICE_UNASSIGNED              = math.MaxUint64

	UNCHANGED = 0
	UPDATED   = 1
	INSERTED  = 2
	REMOVED   = 3
)

// Read a Splice as "at SpAt (in the previous state), SpRemoved elements were removed and SpAdded
// elements were inserted, which can be found starting at SpFrom in the current state"
type Splice struct {
	SpAt      uint64
	SpRemoved uint64
	SpAdded   uint64
	SpFrom    uint64
}

type EditDistanceEqualsFn func(prevIndex uint64, currentIndex uint64) bool

func (s Splice) String() string {
	return fmt.Sprintf("[%d, %d, %d, %d]", s.SpAt, s.SpRemoved, s.SpAdded, s.SpFrom)
}

func uint64Min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func uint64Min3(a, b, c uint64) uint64 {
	if a < b {
		if a < c {
			return a
		}
	} else {
		if b < c {
			return b
		}
	}
	return c
}

func reverse(numbers []uint64) []uint64 {
	newNumbers := make([]uint64, len(numbers))
	for i := 0; i < len(numbers); i++ {
		newNumbers[i] = numbers[len(numbers)-i-1]
	}
	return newNumbers
}

func addSplice(splices []Splice, s Splice) []Splice {
	if s.SpFrom == SPLICE_UNASSIGNED {
		s.SpFrom = 0
	}
	splices = append(splices, s)
	return splices
}

func calcSplices(previousLength uint64, currentLength uint64, maxSpliceMatrixSize uint64, eqFn EditDistanceEqualsFn) []Splice {
	minLength := uint64Min(previousLength, currentLength)
	prefixCount := sharedPrefix(eqFn, minLength)
	suffixCount := sharedSuffix(eqFn, previousLength, currentLength, minLength-prefixCount)

	previousStart := prefixCount
	currentStart := prefixCount
	previousEnd := previousLength - suffixCount
	currentEnd := currentLength - suffixCount

	if (currentEnd-currentStart) == 0 && (previousEnd-previousStart) == 0 {
		return []Splice{}
	}

	if currentStart == currentEnd {
		return []Splice{{previousStart, previousEnd - previousStart, 0, 0}}
	} else if previousStart == previousEnd {
		return []Splice{{previousStart, 0, currentEnd - currentStart, currentStart}}
	}

	previousLength = previousEnd - previousStart
	currentLength = currentEnd - currentStart

	if previousLength*currentLength > maxSpliceMatrixSize {
		return []Splice{{0, previousLength, currentLength, 0}}
	}

	splices := make([]Splice, 0)
	distances := calcEditDistances(eqFn, previousStart, previousLength, currentStart, currentLength)
	ops := operationsFromEditDistances(distances)

	var splice *Splice
	index := currentStart
	previousIndex := previousStart
	for i := 0; i < len(ops); i++ {
		switch ops[i] {
		case UNCHANGED:
			if splice != nil {
				splices = addSplice(splices, *splice)
				splice = nil
			}

			index++
			previousIndex++
			break
		case UPDATED:
			if splice == nil {
				splice = &Splice{index, 0, 0, SPLICE_UNASSIGNED}
			}

			if splice.SpFrom == SPLICE_UNASSIGNED {
				splice.SpFrom = previousIndex
			}

			splice.SpRemoved++
			splice.SpAdded++
			index++
			previousIndex++
			break
		case INSERTED:
			if splice == nil {
				splice = &Splice{index, 0, 0, SPLICE_UNASSIGNED}
			}

			splice.SpAdded++
			if splice.SpFrom == SPLICE_UNASSIGNED {
				splice.SpFrom = previousIndex
			}

			previousIndex++
			break
		case REMOVED:
			if splice == nil {
				splice = &Splice{index, 0, 0, SPLICE_UNASSIGNED}
			}

			splice.SpRemoved++
			index++
			break
		}
	}

	if splice != nil {
		splices = addSplice(splices, *splice)
	}

	return splices
}

func calcEditDistances(eqFn EditDistanceEqualsFn, previousStart uint64, previousLen uint64,
	currentStart uint64, currentLen uint64) [][]uint64 {
	// "Deletion" columns
	rowCount := previousLen + 1
	columnCount := currentLen + 1

	// see https://golang.org/doc/effective_go.html#two_dimensional_slices for below allocation optimization
	distances := make([][]uint64, rowCount)
	distance := make([]uint64, rowCount*columnCount)
	for i := range distances {
		distances[i], distance = distance[:columnCount], distance[columnCount:]
	}

	// "Addition" rows. Initialize null column.
	for i := uint64(0); i < rowCount; i++ {
		distances[i][0] = i
	}

	// Initialize null row
	for j := uint64(0); j < columnCount; j++ {
		distances[0][j] = j
	}

	for i := uint64(1); i < rowCount; i++ {
		for j := uint64(1); j < columnCount; j++ {
			if eqFn(previousStart+i-1, currentStart+j-1) {
				distances[i][j] = distances[i-1][j-1]
			} else {
				north := distances[i-1][j] + 1
				west := distances[i][j-1] + 1
				distances[i][j] = uint64Min(north, west)
			}
		}
	}

	return distances
}

func operationsFromEditDistances(distances [][]uint64) []uint64 {
	i := len(distances) - 1
	j := len(distances[0]) - 1
	current := distances[i][j]
	edits := make([]uint64, 0)
	for i > 0 || j > 0 {
		if i == 0 {
			edits = append(edits, INSERTED)
			j--
			continue
		}
		if j == 0 {
			edits = append(edits, REMOVED)
			i--
			continue
		}
		northWest := distances[i-1][j-1]
		west := distances[i-1][j]
		north := distances[i][j-1]

		minValue := uint64Min3(west, north, northWest)

		if minValue == northWest {
			if northWest == current {
				edits = append(edits, UNCHANGED)
			} else {
				edits = append(edits, UPDATED)
				current = northWest
			}
			i--
			j--
		} else if minValue == west {
			edits = append(edits, REMOVED)
			i--
			current = west
		} else {
			edits = append(edits, INSERTED)
			j--
			current = north
		}
	}

	return reverse(edits)
}

func sharedPrefix(eqFn EditDistanceEqualsFn, searchLength uint64) uint64 {
	for i := uint64(0); i < searchLength; i++ {
		if !eqFn(i, i) {
			return i
		}
	}

	return searchLength
}

func sharedSuffix(eqFn EditDistanceEqualsFn, previousLength uint64, currentLength uint64, searchLength uint64) uint64 {
	count := uint64(0)
	previousLength--
	currentLength--
	for count < searchLength && eqFn(previousLength, currentLength) {
		count++
		previousLength--
		currentLength--
	}

	return count
}
