package types

import (
	"testing"

	"github.com/attic-labs/testify/suite"
)

const (
	lengthOfNumbersTest = 1000
)

type diffTestSuite struct {
	suite.Suite
	col1                Collection
	col2                Collection
	numAddsExpected     int
	numRemovesExpected  int
	numModifiedExpected int
	added               []Value
	removed             []Value
	modified            []Value
}

func newDiffTestSuiteSet(v1 []Value, v2 []Value, numAddsExpected int, numRemovesExpected int, numModifiedExpected int) *diffTestSuite {
	set1 := NewSet(v1...)
	set2 := NewSet(v2...)
	return &diffTestSuite{col1: set1, col2: set2,
		numAddsExpected:     numAddsExpected,
		numRemovesExpected:  numRemovesExpected,
		numModifiedExpected: numModifiedExpected,
	}
}

func newDiffTestSuiteMap(v1 []Value, v2 []Value, numAddsExpected int, numRemovesExpected int, numModifiedExpected int) *diffTestSuite {
	map1 := NewMap(v1...)
	map2 := NewMap(v2...)
	return &diffTestSuite{col1: map1, col2: map2,
		numAddsExpected:     numAddsExpected,
		numRemovesExpected:  numRemovesExpected,
		numModifiedExpected: numModifiedExpected,
	}
}

// Called from testify suite.Run()
func (suite *diffTestSuite) TestDiff() {
	suite.added, suite.removed, suite.modified = orderedSequenceDiff(
		suite.col1.sequence().(orderedSequence),
		suite.col2.sequence().(orderedSequence))
	suite.Equal(suite.numAddsExpected, len(suite.added), "num added is not as expected")
	suite.Equal(suite.numRemovesExpected, len(suite.removed), "num removed is not as expected")
	suite.Equal(suite.numModifiedExpected, len(suite.modified), "num modified is not as expected")
}

// Called from "go test"
func TestOrderedSequencesIdenticalSet(t *testing.T) {
	v1 := generateNumbersAsValuesFromToBy(0, lengthOfNumbersTest, 1)
	v2 := generateNumbersAsValuesFromToBy(0, lengthOfNumbersTest, 1)
	ts := newDiffTestSuiteSet(v1, v2, 0, 0, 0)
	suite.Run(t, ts)
}

func TestOrderedSequencesIdenticalMap(t *testing.T) {
	v1 := generateNumbersAsValuesFromToBy(0, lengthOfNumbersTest*2, 1)
	v2 := generateNumbersAsValuesFromToBy(0, lengthOfNumbersTest*2, 1)
	tm := newDiffTestSuiteMap(v1, v2, 0, 0, 0)
	suite.Run(t, tm)
}

func TestOrderedSequencesSubsetSet(t *testing.T) {
	v1 := generateNumbersAsValuesFromToBy(0, lengthOfNumbersTest, 1)
	v2 := generateNumbersAsValuesFromToBy(0, lengthOfNumbersTest/2, 1)
	ts1 := newDiffTestSuiteSet(v1, v2, 0, lengthOfNumbersTest/2, 0)
	ts2 := newDiffTestSuiteSet(v2, v1, lengthOfNumbersTest/2, 0, 0)
	suite.Run(t, ts1)
	suite.Run(t, ts2)
	ts1.Equal(ts1.added, ts2.removed, "added and removed in reverse order diff")
	ts1.Equal(ts1.removed, ts2.added, "removed and added in reverse order diff")
}

func TestOrderedSequencesSubsetMap(t *testing.T) {
	v1 := generateNumbersAsValuesFromToBy(0, lengthOfNumbersTest*2, 1)
	v2 := generateNumbersAsValuesFromToBy(0, lengthOfNumbersTest, 1)
	tm1 := newDiffTestSuiteMap(v1, v2, 0, lengthOfNumbersTest/2, 0)
	tm2 := newDiffTestSuiteMap(v2, v1, lengthOfNumbersTest/2, 0, 0)
	suite.Run(t, tm1)
	suite.Run(t, tm2)
	tm1.Equal(tm1.added, tm2.removed, "added and removed in reverse order diff")
	tm1.Equal(tm1.removed, tm2.added, "removed and added in reverse order diff")
}

func TestOrderedSequencesDisjointSet(t *testing.T) {
	v1 := generateNumbersAsValuesFromToBy(0, lengthOfNumbersTest, 2)
	v2 := generateNumbersAsValuesFromToBy(1, lengthOfNumbersTest, 2)
	ts1 := newDiffTestSuiteSet(v1, v2, lengthOfNumbersTest/2, lengthOfNumbersTest/2, 0)
	ts2 := newDiffTestSuiteSet(v2, v1, lengthOfNumbersTest/2, lengthOfNumbersTest/2, 0)
	suite.Run(t, ts1)
	suite.Run(t, ts2)

	ts1.Equal(ts1.added, ts2.removed, "added and removed in disjoint diff")
	ts1.Equal(ts1.removed, ts2.added, "removed and added in disjoint diff")
}

func TestOrderedSequencesDisjointMap(t *testing.T) {
	v1 := generateNumbersAsValuesFromToBy(0, lengthOfNumbersTest*2, 2)
	v2 := generateNumbersAsValuesFromToBy(1, lengthOfNumbersTest*2, 2)
	tm1 := newDiffTestSuiteMap(v1, v2, lengthOfNumbersTest/2, lengthOfNumbersTest/2, 0)
	tm2 := newDiffTestSuiteMap(v2, v1, lengthOfNumbersTest/2, lengthOfNumbersTest/2, 0)
	suite.Run(t, tm1)
	suite.Run(t, tm2)
	tm1.Equal(tm1.added, tm2.removed, "added and removed in disjoint diff")
	tm1.Equal(tm1.removed, tm2.added, "removed and added in disjoint diff")
}
