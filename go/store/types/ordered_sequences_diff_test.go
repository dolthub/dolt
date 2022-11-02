// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	lengthOfNumbersTest = 1000
)

type diffFn func(ctx context.Context, last orderedSequence, current orderedSequence, changes chan<- ValueChanged) error

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

func accumulateOrderedSequenceDiffChanges(o1, o2 orderedSequence, df diffFn) (added []Value, removed []Value, modified []Value, err error) {
	changes := make(chan ValueChanged)
	go func() {
		defer close(changes)
		err = df(context.Background(), o1, o2, changes)
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
	return added, removed, modified, err
}

func (suite *diffTestSuite) TestDiff() {
	vs := newTestValueStore()

	type valFn func(*NomsBinFormat, int, int, int) ValueSlice
	type colFn func([]Value) (Collection, error)

	notNil := func(vs []Value) bool {
		for _, v := range vs {
			if v == nil {
				return false
			}
		}
		return true
	}

	runTestDf := func(name string, vf valFn, cf colFn, df diffFn) {
		col1, err := cf(vf(vs.Format(), suite.from1, suite.to1, suite.by1))
		suite.NoError(err)
		col2, err := cf(vf(vs.Format(), suite.from2, suite.to2, suite.by2))
		suite.NoError(err)
		suite.added, suite.removed, suite.modified, err = accumulateOrderedSequenceDiffChanges(
			col1.asSequence().(orderedSequence),
			col2.asSequence().(orderedSequence),
			df)
		suite.NoError(err)
		suite.Equal(suite.numAddsExpected, len(suite.added), "test %s: num added is not as expected", name)
		suite.Equal(suite.numRemovesExpected, len(suite.removed), "test %s: num removed is not as expected", name)
		suite.Equal(suite.numModifiedExpected, len(suite.modified), "test %s: num modified is not as expected", name)
		suite.True(notNil(suite.added), "test %s: added has nil values", name)
		suite.True(notNil(suite.removed), "test %s: removed has nil values", name)
		suite.True(notNil(suite.modified), "test %s: modified has nil values", name)
	}

	runTest := func(name string, vf valFn, cf colFn) {
		runTestDf(name, vf, cf, orderedSequenceDiffLeftRight)
	}

	newSetAsCol := func(vals []Value) (Collection, error) { return NewSet(context.Background(), vs, vals...) }
	newMapAsCol := func(vals []Value) (Collection, error) { return NewMap(context.Background(), vs, vals...) }

	rw := func(col Collection) (Collection, error) {
		ref, err := vs.WriteValue(context.Background(), col)

		if err != nil {
			return nil, err
		}

		h := ref.TargetHash()
		rt, err := vs.Root(context.Background())

		if err != nil {
			return nil, err
		}

		_, err = vs.Commit(context.Background(), rt, rt)

		if err != nil {
			return nil, err
		}

		val, err := vs.ReadValue(context.Background(), h)

		if err != nil {
			return nil, err
		}

		return val.(Collection), nil
	}
	newSetAsColRw := func(vs []Value) (Collection, error) {
		s, err := newSetAsCol(vs)

		if err != nil {
			return nil, err
		}

		return rw(s)
	}
	newMapAsColRw := func(vs []Value) (Collection, error) {
		m, err := newMapAsCol(vs)

		if err != nil {
			return nil, err
		}

		return rw(m)
	}

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
	ts1.True(ts1.added.Equals(ts2.removed), "added and removed in reverse order diff")
	ts1.True(ts1.removed.Equals(ts2.added), "removed and added in reverse order diff")
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
	ts1.True(ts1.added.Equals(ts2.removed), "added and removed in disjoint diff")
	ts1.True(ts1.removed.Equals(ts2.added), "removed and added in disjoint diff")
}

func TestOrderedSequencesDiffCloseWithoutReading(t *testing.T) {
	vs := newTestValueStore()

	runTest := func(t *testing.T, df diffFn) {
		set1, err := NewSet(context.Background(), vs)
		require.NoError(t, err)
		s1 := set1.orderedSequence
		// A single item should be enough, but generate lots anyway.
		set2, err := NewSet(context.Background(), vs, generateNumbersAsValuesFromToBy(vs.Format(), 0, 1000, 1)...)
		require.NoError(t, err)
		s2 := set2.orderedSequence

		changeChan := make(chan ValueChanged)
		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer close(changeChan)
			defer wg.Done()
			err = df(ctx, s1, s2, changeChan)
		}()

		cancel()
		wg.Wait()

		assert.Equal(t, err, context.Canceled)
	}

	t.Run("LeftRight", func(t *testing.T) { runTest(t, orderedSequenceDiffLeftRight) })
}

func TestOrderedSequenceDiffWithMetaNodeGap(t *testing.T) {
	assert := assert.New(t)

	vrw := newTestValueStore()

	newSetSequenceMt := func(v ...Value) (metaTuple, error) {
		seq, err := newSetLeafSequence(vrw, v...)

		if err != nil {
			return metaTuple{}, err
		}

		set := newSet(seq)
		ref, err := vrw.WriteValue(context.Background(), set)

		if err != nil {
			return metaTuple{}, err
		}

		ordKey, err := newOrderedKey(v[len(v)-1], vrw.Format())

		if err != nil {
			return metaTuple{}, err
		}

		return newMetaTuple(ref, ordKey, uint64(len(v)))
	}

	m1, err := newSetSequenceMt(Float(1), Float(2))
	require.NoError(t, err)
	m2, err := newSetSequenceMt(Float(3), Float(4))
	require.NoError(t, err)
	m3, err := newSetSequenceMt(Float(5), Float(6))
	require.NoError(t, err)
	s1, err := newSetMetaSequence(1, []metaTuple{m1, m3}, vrw)
	require.NoError(t, err)
	s2, err := newSetMetaSequence(1, []metaTuple{m1, m2, m3}, vrw)
	require.NoError(t, err)

	runTest := func(df diffFn) {
		var err error
		changes := make(chan ValueChanged)
		go func() {
			defer close(changes)
			err = df(context.Background(), s1, s2, changes)
			if err != nil {
				return
			}
			changes <- ValueChanged{}
			err = df(context.Background(), s2, s1, changes)
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
		require.NoError(t, err)
	}

	runTest(orderedSequenceDiffLeftRight)
}
