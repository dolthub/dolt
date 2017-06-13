// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"testing"

	"github.com/attic-labs/testify/assert"
	"github.com/attic-labs/testify/suite"
)

const (
	lengthOfNumbersTest = 1000
)

type diffFn func(last orderedSequence, current orderedSequence, changes chan<- ValueChanged, closeChan <-chan struct{}) bool

type diffTestSuite struct {
	suite.Suite
	from1, to1, by1     int
	from2, to2, by2     int
	numAddsExpected     int
	numRemovesExpected  int
	numModifiedExpected int
	added               []Value
	removed             []Value
	modified            []Value
}

func newDiffTestSuite(from1, to1, by1, from2, to2, by2, numAddsExpected, numRemovesExpected, numModifiedExpected int) *diffTestSuite {
	return &diffTestSuite{
		from1: from1, to1: to1, by1: by1,
		from2: from2, to2: to2, by2: by2,
		numAddsExpected:     numAddsExpected,
		numRemovesExpected:  numRemovesExpected,
		numModifiedExpected: numModifiedExpected,
	}
}

func accumulateOrderedSequenceDiffChanges(o1, o2 orderedSequence, df diffFn) (added []Value, removed []Value, modified []Value) {
	changes := make(chan ValueChanged)
	closeChan := make(chan struct{})
	go func() {
		df(o1, o2, changes, closeChan)
		close(changes)
	}()
	for change := range changes {
		if change.ChangeType == DiffChangeAdded {
			added = append(added, change.Key)
		} else if change.ChangeType == DiffChangeRemoved {
			removed = append(removed, change.Key)
		} else {
			modified = append(modified, change.Key)
		}
	}
	return
}

func (suite *diffTestSuite) TestDiff() {
	type valFn func(int, int, int) ValueSlice
	type colFn func([]Value) Collection

	notNil := func(vs []Value) bool {
		for _, v := range vs {
			if v == nil {
				return false
			}
		}
		return true
	}

	runTestDf := func(name string, vf valFn, cf colFn, df diffFn) {
		col1 := cf(vf(suite.from1, suite.to1, suite.by1))
		col2 := cf(vf(suite.from2, suite.to2, suite.by2))
		suite.added, suite.removed, suite.modified = accumulateOrderedSequenceDiffChanges(
			col1.sequence().(orderedSequence),
			col2.sequence().(orderedSequence),
			df)
		suite.Equal(suite.numAddsExpected, len(suite.added), "test %s: num added is not as expected", name)
		suite.Equal(suite.numRemovesExpected, len(suite.removed), "test %s: num removed is not as expected", name)
		suite.Equal(suite.numModifiedExpected, len(suite.modified), "test %s: num modified is not as expected", name)
		suite.True(notNil(suite.added), "test %s: added has nil values", name)
		suite.True(notNil(suite.removed), "test %s: removed has nil values", name)
		suite.True(notNil(suite.modified), "test %s: modified has nil values", name)
	}

	runTest := func(name string, vf valFn, cf colFn) {
		runTestDf(name, vf, cf, orderedSequenceDiffTopDown)
		runTestDf(name, vf, cf, orderedSequenceDiffLeftRight)
		runTestDf(name, vf, cf, orderedSequenceDiffBest)
	}

	newSetAsCol := func(vs []Value) Collection { return NewSet(vs...) }
	newMapAsCol := func(vs []Value) Collection { return NewMap(vs...) }

	rw := func(col Collection) Collection {
		vs := newTestValueStore()
		h := vs.WriteValue(col).TargetHash()
		vs.Commit(vs.Root(), vs.Root())
		return vs.ReadValue(h).(Collection)
	}
	newSetAsColRw := func(vs []Value) Collection { return rw(newSetAsCol(vs)) }
	newMapAsColRw := func(vs []Value) Collection { return rw(newMapAsCol(vs)) }

	runTest("set of numbers", generateNumbersAsValuesFromToBy, newSetAsCol)
	runTest("set of numbers (rw)", generateNumbersAsValuesFromToBy, newSetAsColRw)
	runTest("set of structs", generateNumbersAsStructsFromToBy, newSetAsCol)
	runTest("set of structs (rw)", generateNumbersAsStructsFromToBy, newSetAsColRw)

	suite.to1 *= 2
	suite.to2 *= 2
	runTest("map of numbers", generateNumbersAsValuesFromToBy, newMapAsCol)
	runTest("map of structs", generateNumbersAsStructsFromToBy, newMapAsColRw)
	runTest("map of numbers (rw)", generateNumbersAsValuesFromToBy, newMapAsCol)
	runTest("map of structs (rw)", generateNumbersAsStructsFromToBy, newMapAsColRw)
}

func TestOrderedSequencesIdentical(t *testing.T) {
	ts := newDiffTestSuite(
		0, lengthOfNumbersTest, 1,
		0, lengthOfNumbersTest, 1,
		0, 0, 0)
	suite.Run(t, ts)
}

func TestOrderedSequencesSubset(t *testing.T) {
	ts1 := newDiffTestSuite(
		0, lengthOfNumbersTest, 1,
		0, lengthOfNumbersTest/2, 1,
		0, lengthOfNumbersTest/2, 0)
	ts2 := newDiffTestSuite(
		0, lengthOfNumbersTest/2, 1,
		0, lengthOfNumbersTest, 1,
		lengthOfNumbersTest/2, 0, 0)
	suite.Run(t, ts1)
	suite.Run(t, ts2)
	ts1.Equal(ts1.added, ts2.removed, "added and removed in reverse order diff")
	ts1.Equal(ts1.removed, ts2.added, "removed and added in reverse order diff")
}

func TestOrderedSequencesDisjoint(t *testing.T) {
	ts1 := newDiffTestSuite(
		0, lengthOfNumbersTest, 2,
		1, lengthOfNumbersTest, 2,
		lengthOfNumbersTest/2, lengthOfNumbersTest/2, 0)
	ts2 := newDiffTestSuite(
		1, lengthOfNumbersTest, 2,
		0, lengthOfNumbersTest, 2,
		lengthOfNumbersTest/2, lengthOfNumbersTest/2, 0)
	suite.Run(t, ts1)
	suite.Run(t, ts2)
	ts1.Equal(ts1.added, ts2.removed, "added and removed in disjoint diff")
	ts1.Equal(ts1.removed, ts2.added, "removed and added in disjoint diff")
}

func TestOrderedSequencesDiffCloseWithoutReading(t *testing.T) {
	runTest := func(df diffFn) {
		s1 := NewSet().seq
		// A single item should be enough, but generate lots anyway.
		s2 := NewSet(generateNumbersAsValuesFromToBy(0, 1000, 1)...).seq

		changeChan := make(chan ValueChanged)
		closeChan := make(chan struct{})
		stopChan := make(chan struct{})

		go func() {
			df(s1, s2, changeChan, closeChan)
			stopChan <- struct{}{}
		}()

		closeChan <- struct{}{}
		<-stopChan
	}

	runTest(orderedSequenceDiffBest)
	runTest(orderedSequenceDiffLeftRight)
	runTest(orderedSequenceDiffTopDown)
}

func TestOrderedSequenceDiffWithMetaNodeGap(t *testing.T) {
	assert := assert.New(t)

	newSetSequenceMt := func(v ...Value) metaTuple {
		seq := newSetLeafSequence(nil, v...)
		set := newSet(seq)
		return newMetaTuple(NewRef(set), newOrderedKey(v[len(v)-1]), uint64(len(v)), set)
	}

	m1 := newSetSequenceMt(Number(1), Number(2))
	m2 := newSetSequenceMt(Number(3), Number(4))
	m3 := newSetSequenceMt(Number(5), Number(6))
	s1 := newSetMetaSequence(1, []metaTuple{m1, m3}, nil)
	s2 := newSetMetaSequence(1, []metaTuple{m1, m2, m3}, nil)

	runTest := func(df diffFn) {
		changes := make(chan ValueChanged)
		go func() {
			df(s1, s2, changes, nil)
			changes <- ValueChanged{}
			df(s2, s1, changes, nil)
			close(changes)
		}()

		expected := []ValueChanged{
			{DiffChangeAdded, Number(3), nil, nil},
			{DiffChangeAdded, Number(4), nil, nil},
			{},
			{DiffChangeRemoved, Number(3), nil, nil},
			{DiffChangeRemoved, Number(4), nil, nil},
		}

		i := 0
		for c := range changes {
			assert.Equal(expected[i], c)
			i++
		}
		assert.Equal(len(expected), i)
	}

	runTest(orderedSequenceDiffBest)
	runTest(orderedSequenceDiffLeftRight)
	runTest(orderedSequenceDiffTopDown)
}
