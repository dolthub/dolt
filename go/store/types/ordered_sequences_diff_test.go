// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const (
	lengthOfNumbersTest = 1000
)

type diffFn func(ctx context.Context, f *Format, last orderedSequence, current orderedSequence, changes chan<- ValueChanged, closeChan <-chan struct{}) bool

type diffTestSuite struct {
	suite.Suite
	from1, to1, by1     int
	from2, to2, by2     int
	numAddsExpected     int
	numRemovesExpected  int
	numModifiedExpected int
	added               ValueSlice
	removed             ValueSlice
	modified            ValueSlice
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
		df(context.Background(), Format_7_18, o1, o2, changes, closeChan)
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
	vs := newTestValueStore()

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
			col1.asSequence().(orderedSequence),
			col2.asSequence().(orderedSequence),
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

	newSetAsCol := func(vals []Value) Collection { return NewSet(context.Background(), Format_7_18, vs, vals...) }
	newMapAsCol := func(vals []Value) Collection { return NewMap(context.Background(), Format_7_18, vs, vals...) }

	rw := func(col Collection) Collection {
		h := vs.WriteValue(context.Background(), col).TargetHash()
		vs.Commit(context.Background(), vs.Root(context.Background()), vs.Root(context.Background()))
		return vs.ReadValue(context.Background(), h).(Collection)
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
	ts1.True(ts1.added.Equals(Format_7_18, ts2.removed), "added and removed in reverse order diff")
	ts1.True(ts1.removed.Equals(Format_7_18, ts2.added), "removed and added in reverse order diff")
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
	ts1.True(ts1.added.Equals(Format_7_18, ts2.removed), "added and removed in disjoint diff")
	ts1.True(ts1.removed.Equals(Format_7_18, ts2.added), "removed and added in disjoint diff")
}

func TestOrderedSequencesDiffCloseWithoutReading(t *testing.T) {
	vs := newTestValueStore()

	runTest := func(df diffFn) {
		s1 := NewSet(context.Background(), Format_7_18, vs).orderedSequence
		// A single item should be enough, but generate lots anyway.
		s2 := NewSet(context.Background(), Format_7_18, vs, generateNumbersAsValuesFromToBy(0, 1000, 1)...).orderedSequence

		changeChan := make(chan ValueChanged)
		closeChan := make(chan struct{})
		stopChan := make(chan struct{})

		go func() {
			df(context.Background(), Format_7_18, s1, s2, changeChan, closeChan)
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

	vrw := newTestValueStore()

	newSetSequenceMt := func(v ...Value) metaTuple {
		seq := newSetLeafSequence(Format_7_18, vrw, v...)
		set := newSet(Format_7_18, seq)
		return newMetaTuple(Format_7_18, vrw.WriteValue(context.Background(), set), newOrderedKey(v[len(v)-1], Format_7_18), uint64(len(v)))
	}

	m1 := newSetSequenceMt(Float(1), Float(2))
	m2 := newSetSequenceMt(Float(3), Float(4))
	m3 := newSetSequenceMt(Float(5), Float(6))
	s1 := newSetMetaSequence(1, []metaTuple{m1, m3}, Format_7_18, vrw)
	s2 := newSetMetaSequence(1, []metaTuple{m1, m2, m3}, Format_7_18, vrw)

	runTest := func(df diffFn) {
		changes := make(chan ValueChanged)
		go func() {
			df(context.Background(), Format_7_18, s1, s2, changes, nil)
			changes <- ValueChanged{}
			df(context.Background(), Format_7_18, s2, s1, changes, nil)
			close(changes)
		}()

		expected := []ValueChanged{
			{DiffChangeAdded, Float(3), nil, nil},
			{DiffChangeAdded, Float(4), nil, nil},
			{},
			{DiffChangeRemoved, Float(3), nil, nil},
			{DiffChangeRemoved, Float(4), nil, nil},
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
