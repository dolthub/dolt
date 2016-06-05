// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"testing"

	"github.com/attic-labs/testify/assert"
)

func assertDiff(assert *assert.Assertions, last []uint64, current []uint64, expect []Splice) {
	actual := calcSplices(uint64(len(last)), uint64(len(current)), func(i uint64, j uint64) bool { return last[i] == current[j] })
	assert.Equal(expect, actual, "splices are different: \nexpect: %v\nactual: %v\n", expect, actual)
}

func TestEditDistanceAppend(t *testing.T) {
	assert := assert.New(t)
	assertDiff(assert,
		[]uint64{0, 1, 2},
		[]uint64{0, 1, 2, 3, 4, 5},
		[]Splice{Splice{3, 0, 3, 3}},
	)
}

func TestEditDistancePrepend(t *testing.T) {
	assert := assert.New(t)
	assertDiff(assert,
		[]uint64{3, 4, 5, 6},
		[]uint64{0, 1, 2, 3, 4, 5, 6},
		[]Splice{Splice{0, 0, 3, 0}},
	)
}

func TestEditDistanceChopFromEnd(t *testing.T) {
	assert := assert.New(t)
	assertDiff(assert,
		[]uint64{0, 1, 2, 3, 4, 5},
		[]uint64{0, 1, 2},
		[]Splice{Splice{3, 3, 0, 0}},
	)
}

func TestEditDistanceChopFromStart(t *testing.T) {
	assert := assert.New(t)
	assertDiff(assert,
		[]uint64{0, 1, 2, 3, 4, 5},
		[]uint64{3, 4, 5},
		[]Splice{Splice{0, 3, 0, 0}},
	)
}

func TestEditDistanceChopFromMiddle(t *testing.T) {
	assert := assert.New(t)
	assertDiff(assert,
		[]uint64{0, 1, 2, 3, 4, 5},
		[]uint64{0, 5},
		[]Splice{Splice{1, 4, 0, 0}},
	)
}

func TestEditDistanceA(t *testing.T) {
	assert := assert.New(t)
	assertDiff(assert,
		[]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8},
		[]uint64{0, 1, 2, 4, 5, 6, 8},
		[]Splice{
			Splice{3, 1, 0, 0},
			Splice{7, 1, 0, 0},
		},
	)
}

func TestEditDistanceRemoveABunch(t *testing.T) {
	assert := assert.New(t)
	assertDiff(assert,
		[]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		[]uint64{1, 2, 4, 5, 7, 8, 10},
		[]Splice{
			Splice{0, 1, 0, 0},
			Splice{3, 1, 0, 0},
			Splice{6, 1, 0, 0},
			Splice{9, 1, 0, 0},
		},
	)
}

func TestEditDistanceAddABunch(t *testing.T) {
	assert := assert.New(t)
	assertDiff(assert,
		[]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		[]uint64{0, 'a', 1, 2, 3, 'b', 'c', 'd', 4, 5, 6, 7, 'e', 8, 9, 'f', 10, 'g'},
		[]Splice{
			Splice{1, 0, 1, 1},
			Splice{4, 0, 3, 5},
			Splice{8, 0, 1, 12},
			Splice{10, 0, 1, 15},
			Splice{11, 0, 1, 17},
		},
	)
}

func TestEditDistanceUpdateABunch(t *testing.T) {
	assert := assert.New(t)
	assertDiff(assert,
		[]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		[]uint64{'a', 1, 2, 'b', 'c', 'd', 6, 7, 'e', 9, 10},
		[]Splice{
			Splice{0, 1, 1, 0},
			Splice{3, 3, 3, 3},
			Splice{8, 1, 1, 8},
		},
	)
}

func TestEditDistanceLeftOverlap(t *testing.T) {
	assert := assert.New(t)
	assertDiff(assert,
		[]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		[]uint64{0, 1, 2, 3, 'a', 'b', 8, 9, 10},
		[]Splice{
			Splice{4, 4, 2, 4},
		},
	)
}

func TestEditDistanceRightOverlap(t *testing.T) {
	assert := assert.New(t)
	assertDiff(assert,
		[]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		[]uint64{0, 1, 2, 3, 4, 5, 'a', 'b', 10},
		[]Splice{
			Splice{6, 4, 2, 6},
		},
	)
}

func TestEditDistanceWithin(t *testing.T) {
	assert := assert.New(t)
	assertDiff(assert,
		[]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		[]uint64{0, 1, 2, 3, 'a', 'b', 10},
		[]Splice{
			Splice{4, 6, 2, 4},
		},
	)
}

func TestEditDistanceWithout(t *testing.T) {
	assert := assert.New(t)
	assertDiff(assert,
		[]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		[]uint64{0, 1, 2, 3, 4, 5, 'a', 'b', 'c', 'd', 8, 9, 10},
		[]Splice{
			Splice{6, 2, 4, 6},
		},
	)
}

func TestEditDistanceMix1(t *testing.T) {
	assert := assert.New(t)
	assertDiff(assert,
		[]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		[]uint64{0, 'a', 1, 'b', 3, 'c', 4, 6, 7, 'e', 'f', 10},
		[]Splice{
			Splice{1, 0, 1, 1},
			Splice{2, 1, 1, 3},
			Splice{4, 0, 1, 5},
			Splice{5, 1, 0, 0},
			Splice{8, 2, 2, 9},
		},
	)
}

func TestEditDistanceReverse(t *testing.T) {
	assert := assert.New(t)
	assertDiff(assert,
		[]uint64{0, 1, 2, 3, 4, 5, 6, 7},
		[]uint64{7, 6, 5, 4, 3, 2, 1, 0},
		[]Splice{
			Splice{0, 3, 4, 0},
			Splice{4, 4, 3, 5},
		},
	)
}
